import { doc, setDoc, getDoc, deleteDoc } from "https://www.gstatic.com/firebasejs/10.13.2/firebase-firestore.js";
import { db } from "./firebase-init.js";

// Firestore caps a single document at ~1MiB. We stay well under that per
// chunk to leave headroom for field-name/BSON-style overhead, and because
// JS string .length counts UTF-16 code units, not the UTF-8 bytes Firestore
// actually bills against (Thai text is ~3 bytes/char) — so we measure real
// byte size with TextEncoder rather than trusting .length.
const CHUNK_BYTE_BUDGET = 700000;
const enc = new TextEncoder();
function byteLen(v){ return enc.encode(JSON.stringify(v)).length; }

function buildChunks(rows){
  var chunks=[], current=[];
  for(var i=0;i<rows.length;i++){
    current.push(rows[i]);
    if(byteLen(current) > CHUNK_BYTE_BUDGET){
      var last=current.pop();
      if(current.length) chunks.push(current);
      current=[last];
    }
  }
  if(current.length) chunks.push(current);
  return chunks;
}

// Overwrites the previous cloud copy with the given rows (chunked as needed),
// then deletes any leftover chunk docs from a larger previous upload.
export async function fbPushStock(rows, srcName){
  var metaSnap = await getDoc(doc(db,'stock_sync','meta'));
  var oldChunkCount = metaSnap.exists() ? (metaSnap.data().chunkCount||0) : 0;
  var chunks = buildChunks(rows);
  var writes = chunks.map(function(chunk,i){
    return setDoc(doc(db,'stock_sync','chunk_'+i), { rows: chunk });
  });
  await Promise.all(writes);
  await setDoc(doc(db,'stock_sync','meta'), {
    chunkCount: chunks.length,
    totalCount: rows.length,
    ts: Date.now(),
    src: srcName || ''
  });
  var cleanup=[];
  for(var j=chunks.length;j<oldChunkCount;j++){
    cleanup.push(deleteDoc(doc(db,'stock_sync','chunk_'+j)));
  }
  await Promise.all(cleanup);
  return { chunkCount: chunks.length, totalCount: rows.length };
}

// Deletes all cloud chunks and resets meta to empty (mirrors a local "clear all").
export async function fbClearStock(){
  var metaSnap = await getDoc(doc(db,'stock_sync','meta'));
  var oldChunkCount = metaSnap.exists() ? (metaSnap.data().chunkCount||0) : 0;
  var cleanup=[];
  for(var j=0;j<oldChunkCount;j++){
    cleanup.push(deleteDoc(doc(db,'stock_sync','chunk_'+j)));
  }
  await Promise.all(cleanup);
  await setDoc(doc(db,'stock_sync','meta'), { chunkCount:0, totalCount:0, ts:Date.now(), src:'' });
}

// Reads the full stock array back out of its chunks.
export async function fbLoadStock(){
  var metaSnap = await getDoc(doc(db,'stock_sync','meta'));
  if(!metaSnap.exists()) return { data:[], ts:null, src:'' };
  var meta = metaSnap.data();
  var chunkCount = meta.chunkCount||0;
  if(!chunkCount) return { data:[], ts:meta.ts||null, src:meta.src||'' };
  var reads=[];
  for(var i=0;i<chunkCount;i++){
    reads.push(getDoc(doc(db,'stock_sync','chunk_'+i)));
  }
  var snaps = await Promise.all(reads);
  var data=[];
  snaps.forEach(function(s){ if(s.exists()) data=data.concat(s.data().rows||[]); });
  return { data:data, ts:meta.ts||null, src:meta.src||'' };
}
