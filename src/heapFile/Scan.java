package heapFile;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

import chainexception.ChainException;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;

public class Scan {
	private HFPage curHFpage;
	private RID curRID;
	private boolean first;
	private PageId curPageId = new PageId();

	public Scan(HFPage now) {
		// TODO Auto-generated constructor stub
		try {
			this.curHFpage = now;
			curRID = curHFpage.firstRecord();
			first = true;
			curPageId.copyPageId(curHFpage.getCurPage());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			HFPage myPage = new HFPage();
			SystemDefs.JavabaseBM.pinPage(now.getCurPage(), myPage, false);
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageNotReadException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufferPoolExceededException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PagePinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Tuple getNext(RID rid) throws ChainException, IOException {
		Tuple t = null;
		if (first) {
			curRID =curHFpage.firstRecord();
			t = curHFpage.getRecord(curRID);
			rid.copyRid(curRID);
			first = false;
			return t;
		} else {
			curRID = curHFpage.nextRecord(curRID);
			if (curRID == null) {
				SystemDefs.JavabaseBM.unpinPage(curPageId, true);
				curPageId = curHFpage.getNextPage();
				if (curPageId.pid == -1) {
					return null;
				} else {
					SystemDefs.JavabaseBM.pinPage(curPageId, curHFpage, false);
					curRID = curHFpage.firstRecord();
					if(curRID==null){
						SystemDefs.JavabaseBM.unpinPage(curPageId, true);
						return null;
					}
					rid.copyRid(curRID);
					t=curHFpage.getRecord(curRID);
					return t;
				}
 
			}else{
				rid.copyRid(curRID);
				t=curHFpage.getRecord(curRID);
				return t;
			}
		}
 
	}// end method.
 

	public void closescan() {
		// TODO Auto-generated method stub
		try {
			curHFpage = null;
			curRID = null;
			curPageId = null;
			first = true;
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
