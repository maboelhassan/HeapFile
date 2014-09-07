package heapFile;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import chainexception.ChainException;
import global.*;
import heap.*;
import diskmgr.*;

public class Heapfile {
	private HFPage page;
	private int recCnt;
	private Page first;
	private PageId pid;
	private String name;
	public Heapfile(String string) throws FileNameTooLongException,
			InvalidPageNumberException, InvalidRunSizeException,
			DuplicateEntryException, OutOfSpaceException, FileIOException,
			DiskMgrException, IOException, ChainException,
			HashOperationException, PageUnpinnedException,
			InvalidFrameNumberException, PageNotReadException,
			BufferPoolExceededException, PagePinnedException, BufMgrException {
		page = new HFPage();
		recCnt = 0;
		SystemDefs.JavabaseDB.openDB(SystemDefs.JavabaseDBName);
		// TODO Auto-generated constructor stub
		first = new Page();
		pid = new PageId(0);
	    this.name = string;
		if (SystemDefs.JavabaseDB.get_file_entry(string) == null) {
			try {
				pid = SystemDefs.JavabaseBM.newPage(first, 1);
				SystemDefs.JavabaseDB.add_file_entry(string, pid);
				page.init(pid, first);
				page.setPrevPage(new PageId(-1));
				SystemDefs.JavabaseBM.unpinPage(pid, true);
			} catch (BufferPoolExceededException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HashOperationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ReplacerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HashEntryNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidFrameNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PagePinnedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PageUnpinnedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (PageNotReadException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BufMgrException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			PageId firstPid = (SystemDefs.JavabaseDB.get_file_entry(string));
			HFPage toOpen = new HFPage();

			SystemDefs.JavabaseBM.pinPage(firstPid, toOpen, true);
			SystemDefs.JavabaseBM.unpinPage(firstPid, false);

			page.openHFpage(toOpen);
			page.setPrevPage(new PageId(-1));

		}
	}

	public Scan openScan() throws IOException, FileIOException, InvalidPageNumberException, DiskMgrException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		// TODO Auto-generated method stub
		PageId firstPid = (SystemDefs.JavabaseDB.get_file_entry(name));
		HFPage toOpen = new HFPage();

		SystemDefs.JavabaseBM.pinPage(firstPid, toOpen, true);
		SystemDefs.JavabaseBM.unpinPage(firstPid, false);

		Scan scan = new Scan(toOpen);
		return scan;
	}

	public int getRecCnt() {
		// TODO Auto-generated method stub
		return recCnt;
	}
	public boolean deleteRecord(RID rid) throws IOException, ChainException {

		HFPage tempPage = new HFPage();
		PageId tempPageId = page.getCurPage();
		while (tempPageId.pid != -1) {
			SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			if (tempPageId.pid == rid.pageNo.pid) {
				tempPage.deleteRecord(rid);
				SystemDefs.JavabaseBM.unpinPage(tempPageId, true);
				recCnt--;
				return true;
			}// end if.
			SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
			tempPageId = tempPage.getNextPage();
		}// end while.
		return false;
	}// end method.

	public RID insertRecord(byte recPtr[]) throws IOException, ChainException {
		RID rid = new RID();
		if (recPtr.length > GlobalConst.MAX_SPACE) {
			throw new SpaceNotAvailableException(null, "SPACE_NOT_AVAILABLE");
		}

		HFPage tempPage = new HFPage();
		PageId tempPageId = page.getCurPage();
		while (tempPageId.pid != -1) {
			SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			if (tempPage.available_space() >= recPtr.length) {
				RID tempRid = tempPage.insertRecord(recPtr);
				rid.pageNo.pid = tempRid.pageNo.pid;
				rid.slotNo = tempRid.slotNo;
				recCnt++;
				SystemDefs.JavabaseBM.unpinPage(tempPageId, true);
				return rid;
			}// end if.

			SystemDefs.JavabaseBM.unpinPage(tempPageId, true);
			tempPageId = tempPage.getNextPage();
		}// end while.

		// me7tag a3ml Page gdeda.
		// recCnt++;
		HFPage newPage = new HFPage();
		PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage, 1);

		newPage.init(newPageId, newPage);
		// SystemDefs.JavabaseBM.unpinPage(newPageId, true);

		// SystemDefs.JavabaseBM.pinPage(newPageId, newPage, false);
		RID tempRid2 = newPage.insertRecord(recPtr);
		rid.pageNo.pid = tempRid2.pageNo.pid;
		rid.slotNo = tempRid2.slotNo;
		SystemDefs.JavabaseBM.unpinPage(newPageId, true);

		// azabat ba2a el next w el previous: bas hena el previous ba2a.
		tempPageId = page.getCurPage();
		tempPage = new HFPage();
		SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
		SystemDefs.JavabaseBM.unpinPage(tempPageId, true);
		while (tempPage.getNextPage().pid != -1) {
			tempPageId = tempPage.getNextPage();
			SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			SystemDefs.JavabaseBM.unpinPage(tempPageId, true);

		}// end while.

		newPage.setPrevPage(tempPage.getCurPage());
		tempPage.setNextPage(newPage.getCurPage());

		recCnt++;
		return rid;
	}// end method.

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException, IOException {
		// TODO Auto-generated method stub
		HFPage tempPage = new HFPage();
		PageId tempPageId = page.getCurPage();
		while (tempPageId.pid != -1) {
			SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			if (tempPageId.pid == rid.pageNo.pid) {
				Tuple tuple = new Tuple();
				tuple  = tempPage.getRecord(rid);
				if(tuple.getLength() != newTuple.getLength())
				{
					SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
					throw new InvalidUpdateException(null, "heap.InvalidUpdateException");
				}
				else
				{
					tuple = tempPage.returnRecord(rid);
					tuple.tupleCopy(newTuple);
					SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
					return true;
				}
				
			}// end if.
			SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
			tempPageId = tempPage.getNextPage();
		}// end while.
		return false ; 
	}


	
	
	public Tuple getRecord(RID rid) throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, InvalidSlotNumberException, HashEntryNotFoundException{


		HFPage tempPage = new HFPage();
		PageId tempPageId = page.getCurPage();
		while (tempPageId.pid != -1) {
			SystemDefs.JavabaseBM.pinPage(tempPageId, tempPage, false);
			if (tempPageId.pid == rid.pageNo.pid) {
				Tuple newTuple = new Tuple();
				newTuple  = tempPage.getRecord(rid);
				SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
				return newTuple;
			}// end if.
			SystemDefs.JavabaseBM.unpinPage(tempPageId, false);
			tempPageId = tempPage.getNextPage();
		}// end while.
		return null ; 

	}
	
	
	
	

}