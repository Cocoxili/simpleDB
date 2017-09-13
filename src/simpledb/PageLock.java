package simpledb;


public class PageLock {

	public static int maxlocknum = 64;
	private TransactionId [] Tid;
	private PageId [] Pid;
	private int [] ll; 
	
	public PageLock(){
		Tid = new TransactionId [maxlocknum];
		Pid = new PageId [maxlocknum];
		ll = new int [maxlocknum];
		for(int itx = 0 ; itx < maxlocknum ; itx ++){
			ll[itx] = -1;
		}
	}
	
	public boolean readLockable(TransactionId tid, PageId pid){
		int itx;
		//this page is not readLockable when writeLocked by other transaction
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Pid[itx] != null && Pid[itx].equals(pid)){
				if(ll[itx] == 2 && ! Tid[itx].equals(tid)) return false;
			}
		}
		return true;
	}
	
	public boolean writeLockable(TransactionId tid, PageId pid){
		int itx;
		//not writelockable when locked by other transaction
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Pid[itx] != null && Pid[itx].equals(pid)){
				if(!Tid[itx].equals(tid)) return false;
			}
		}
		return true;
	}
	
	public void lockPage(TransactionId tid, PageId pid, Permissions perm){
		int itx;
		if(lockLevel(tid, pid) == -1){
			for(itx = 0 ; itx < maxlocknum ; itx ++){
				if(ll[itx] == -1){
					Pid[itx] = pid;
					Tid[itx] = tid;
					ll[itx] = (perm == Permissions.READ_ONLY) ? 1 : 2;
					return;
				}
			}
		}
		if(lockLevel(tid, pid) == ((perm == Permissions.READ_ONLY) ? 1 : 2))
			return;
		if(lockLevel(tid, pid) == 1 &&  perm == Permissions.READ_WRITE){
			int loc = lockLoc(tid, pid);
			ll[loc] = 2;
		}
	}
	
	public void releaseLock(TransactionId tid, PageId pid){
		int itx;
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Tid[itx] != null && Pid[itx] != null &&
			   tid.equals(Tid[itx]) && pid.equals(Pid[itx])){
				Tid[itx] = null;
				Pid[itx] = null;
				ll[itx] = -1;
				break;
			}
		}		
	}

	public void releaseLockTrans(TransactionId tid){
		int itx;
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Tid[itx] != null && Tid[itx].equals(tid)){
				Tid[itx] = null;
				Pid[itx] = null;
				ll[itx] = -1;
			}
		}
	}
	
	public boolean writeLocked(PageId pid){
		int itx;
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Pid[itx] != null && Pid[itx].equals(pid) && ll[itx] == 2){
				return true;
			}
		}
		return false;
	}
	
	public int lockLevel(TransactionId tid, PageId pid){
		int itx;
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Tid[itx] != null)
				if(Tid[itx].equals(tid) && Pid[itx].equals(pid))
					return ll[itx];
		}
		return -1;
	}
	
	public int lockLoc(TransactionId tid, PageId pid){
		int itx;
		for(itx = 0 ; itx < maxlocknum ; itx ++){
			if(Tid[itx] != null)
				if(Tid[itx].equals(tid) && Pid[itx].equals(pid))
					return itx;
		}
		return -1;
	}
}
