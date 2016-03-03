#include "LogRecord.h"
#include <vector>
#include "../StorageEngine/StorageEngine.h"
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include "LogMgr.h"
#include <queue>

using namespace std;


int LogMgr::getLastLSN(int txnum){

   
    if(tx_table.find(txnum) == tx_table.end())
    	return NULL_LSN;
    else return tx_table[txnum].lastLSN;
}

void LogMgr::setLastLSN(int txnum,int lsn){
     if(tx_table.find(txnum) == tx_table.end())
     {
     	tx_Table[txnum]=txTableEntry(lsn,TxStatus::U);
     	return;

     }
     else tx_table[txnum].lastLSN =lsn;
     	return;

}


void LogMgr::flushlogTail(int maxLSN){
	string temp="";
	if(logtail.empty()) return;
	if(logtail[0]->getLSN()>maxLSN) return;
	int i= 0;
	while( logtail.size()>i  &&   (logtail[i]->getLSN()<=maxLSN)){
		temp += logtail[i++]->toString();
		delete logtail[i-1];
		}
	se->updateLog(temp);
	logtail.erase(logtail.begin(),logtail.begin()+i);
	return;
	
}

void analyze(vector <LogRecord*> log){
  	int i = 0;
  	if (se->get_master()!=-1){
  		for(int k=0;k<log.size();k++){
  			if(log[k]->getLSN()==(se->get_master())
  				i =k;
  			break;
  		}
  		ChkptLogRecord *chk_ptr = dynamic_cast<ChkptLogRecord *> (log[i]);
  		tx_table = chk_ptr->getTxTable();
  		dirty_page_table =chk_ptr->getDirtyPageTable();
  	}
	for(i+1; i<log.size();i++){
 	 	if(log[i]->getType()==TxType::UPDATE){
 	 		UpdateLogRecord *update_ptr =dynamic_cast<UpdateLogRecord *>(log[i]);
	 		setLastLSN(update_ptr->getTxID(),update_ptr->getLSN());
 	 		if(dirty_page_table.find(update_ptr->getPageID())==dirty_page_table.end())
   			 dirty_page_table[update_ptr->getPageID()]=update_ptr->getLSN();
 	 	}
 	 	else if(log[i]->getType()==TxType::CLR){
 	 		CompensationLogRecord *clr_ptr =dynamic_cast<CompensationLogRecord *>(log[i]);
	 		setLastLSN(clr_ptr->getTxID(),clr_ptr->getLSN());
 	 		if(dirty_page_table.find(clr_ptr->getPageID())==dirty_page_table.end())
   			 dirty_page_table[clr_ptr->getPageID()]=clr_ptr->getLSN();

 	 	}
 	 	else if(log[i]->getType() ==TxType::COMMIT){
 	 		setLastLSN(log[i].getTxID(),log[i].getLSN());
 	 		tx_table.find(log[i].getTxID())->second.status =TxStatus::C;
 	 	}
		else if(log[i]->getType() ==TxType::ABORT){
 	 		setLastLSN(log[i].getTxID(),log[i].getLSN());
 	 	}

 	 	else if(log[i]->getType() == TxType::END){
 	 		tx_table.erase(log[i]->getTxID());
 	 	}

	}
  	
  	
}
bool redo(vector <LogRecord*> log){
	//find the smallest recLSN
	int smallestLSN =dirty_page_table.begin()->second;
	for(std::map<int,int>::iterator it = dirty_page_table.begin();it!=dirty_page_table.end();it++){
		if it->second<smallestLSN
			smallestLSN = it->second;
	}
	//start at log[i]
	int i = 0 ;
	for(int k=0;k<log.size();k++){
  			if(log[k]->getLSN()==smallestLSN)
  				i =k;
  			break;
  		}

	for(i;i<log.size();i++){
		bool result;

		if (log[i]->getType == TxType::UPDATE){
			UpdateLogRecord *update_ptr = dynamic_cast<UpdateLogRecord *> (log[i]);
			redo_page_id = update_ptr->getPageID();
			redo_lsn =update_ptr->getLSN();
			redo_offset =update_ptr->getOffset();
			redo_image =update_ptr->getAfterImage();
			page_lsn =se->getLSN(redo_page_id);
			if(dirty_page_table[redo_page_id]!=dirty_page_table.end() && dirty_page_table[redo_page_id]<=redo_lsn && page_lsn<=redo_lsn )
				result = se->pageWrite(redo_page_id, redo_offset, redo_image, redo_lsn);
			if(!result)
				return false;
		}
		else if (log[i]->getType ==TxType::CLR){
			CompensationLogRecord *clr_ptr = dynamic_cast<CompensationLogRecord *> (log[i]);
			redo_page_id = clr_ptr->getPageID();
			redo_lsn =clr_ptr->getLSN();
			redo_offset =clr_ptr->getOffset();
			redo_image =clr_ptr->getAfterImage();
			page_lsn =se->getLSN(redo_page_id);
			if(dirty_page_table[redo_page_id]!=dirty_page_table.end() && dirty_page_table[redo_page_id]<=redo_lsn && page_lsn<=redo_lsn )
				result =se->pageWrite(redo_page_id, redo_offset, redo_image, redo_lsn);
			if(!result)
				return false;

		}
		else if(log[i]->getType()==TxType::COMMIT ){
			if(tx_table[log[i]->getTxID()] !=tx_table.end()){
				LogRecord *end_log = new LogRecord(se->nextLSN(), getLastLSN(log[i]->getTxID()), log[i]->getTxID(), TxType::END);
				logtail.push_back(end_log);
				tx_table.erase(log[i]->getTxID());

			}
		}

		
	}
	return true;

}
void undo(vector <LogRecord*> log, int txnum = NULL_TX){
  	//save lastLSN as a priority queue;
  	std::priority_queue<int>toUndo;
  	if(txnum == NULL_TX){
  		for(std::map <int, txTableEntry>::iterator it =tx_table.begin();it!=tx_table.end();it++){
  			toUndo.push((it->second).lastLSN);
  		}
  	}
  	else toUndo.push(getLSN(txnum));
  	
  	//start at the lastLSN
  	while(!toUndo.empty()){
  		int now_lsn = toUndo.top();
  		int i =0;
  		for(int k=0;k<log.size();k++){
  			if(log[k]->getLSN()==now_lsn)
  				i =k;
  			break;
  		}
  		//if update,create a clr record, do pageWrite, if prev is -1 create end and erase,else update toUndo
  		if(log[i]->getType()==TxType::UPDATE){
  			UpdateLogRecord *update_ptr = dynamic_cast<UpdateLogRecord *> (log[i]);
  			
  			lsn_here =se->nextLSN();
  			tx_id =update_ptr->getTxID();
  			last_lsn =update_ptr->getLastLSN(tx_id);
  			undo_page_id =update_ptr->getPageID();
  			undo_offset =update_ptr->getOffset();
  			undo_text =update_ptr->getBeforeImage();
  			undoNextLSN =update_ptr->getprevLSN();
  			CompensationLogRecord* undo_clr = new CompensationLogRecord(lsn_here, last_lsn, tx_id, undo_page_id, undo_offset,undo_text, undoNextLSN)
  			logtail.pushback(undo_clr); 
  			
  			se->pageWrite(undo_page_id, undo_offset, undo_text, lsn_here);
  			setLastLSN(tx_id,lsn_here);
  			
  			toUndo.pop();
  			if(undoNextLSN!= NULL_LSN){
  				toUndo.push(undoNextLSN);
  			}
  			else{
  				LogRecord *end_log = new LogRecord(se->nextLSN(), lsn_here, tx_id, TxType::END);
				logtail.push_back(end_log);
				tx_table.erase(tx_id);
  			}
  		}
  		//if CLR, if toUndoNext==-1,pop
  		else if(log[i]->getType()==TxType::CLR){
				CompensationLogRecord *clr_ptr = dynamic_cast<CompensationLogRecord *> (log[i]);

  			if(clr_ptr->getUndoNextLSN()==NULL_LSN){
  				LogRecord *end_log = new LogRecord(se->nextLSN(), getLastLSN(clr_ptr->getTxID()), clr_ptr->getTxID(), TxType::END);
				logtail.push_back(end_log);
				tx_table.erase(clr_ptr->getTxID());
				toUndo.pop();
  			}
  			else{
  				toUndo.pop();
  				toUndo.push(clr_ptr->getUndoNextLSN());
  			}

  		}
  		//only meet this sitution when its a abort transaction
  		else if(log[i]->getType == TxType::ABORT){
  			toUndo.pop();
  			toUndo.push(log[i].getprevLSN());

  		}

  	}


}



vector<LogRecord*> LogMgr::stringToLRVector(string logstring){
	vector<LogRecord*> result; 
	istringstream stream(logstring);
	string line; 
	while(getline(stream, line)) { 
		LogRecord* lr =LogRecord::stringToRecordPtr(line); 
		result.push_back(lr);
	}
	return result;
}

void abort(int txid){
	//create a ABORT logrecord
	int lsn_abort = se->nextLSN();
  	logtail.pushback(LogRecord(lsn_abort, getLastLSN(txid), txid, TxType::ABORT));
  	setLastLSN(txid,lsn_abort);
 
  	 vector <LogRecord*> logOnDisk = stringToLRVector(se->getLog());
  	 logOnDisk.insert(logOnDisk.end(),logtail.begin(),logtail.end());
  	undo(logOnDisk,txid);

}; 

void LogMgr::checkpoint(){ 
	int now_lsn = se->nextLSN();
	int last_lsN=NULL_LSN;
	int tx_num =NULL_TX;
	//create begin checkpoint
	LogRecord *ckp_begin= new LogRecord(now_lsn, last_lsn, tx_num, TxType::BEGIN_CKPT);
	logtail.push_back(ckp_begin);
	
	//create end checkpoint
	last_lsn= now_lsn;
	now_lsn =se->nextLSN();
	ChkptLogRecord *ckp_end =new ChkptLogRecord(now_lsn, last_lsn, tx_num, tx_table, dirty_page_table);
	logtail.push_back(ckp_end);
	se->store_master(now_lsn);
	
	flushlogTail(now_lsn);
 	


}

void commit(int txid){
	int now_lsn = se->nextLSN();
	int last_lsn = getLastLSN(txid);
	
	LogRecord *commit_log = new LogRecord(now_lsn, last_lsn, txid, TxType::COMMIT);
	logtail.push_back(commit_log);
	flushlogTail(now_lsn);
	
	last_lsn =now_lsn;
	now_lsn=se->nextLSN();
	LogRecord *end_log = new LogRecord(now_lsn, last_lsn, txid, TxType::END);
	logtail.push_back(end_log);
	tx_table.erase(txid);
    return;

}

void LogMgr::pageFlushed(int page_id){
	
	flushlogTail(se->getLSN(page_id));
	dirty_page_table.erase(page_id);
	return;

}

void LogMgr::recover(string log){
	std::vector<LogRecord*> logStory = stringToLRVector(log);
	analyze(logStory);
	redo(logStory);
	undo(logStory);


}


int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext){
	int now_lsn = se->nextLSN();
	int last_lsn = getLastLSN(txid);
    //update tx_table
    setLastLSN(txid,now_lsn);
    //update dirty_page_table
    if(dirty_page_table.find(page_id)==dirty_page_table.end())
    dirty_page_table[page_id]=now_lsn;
	UpdateLogRecord * upld = new UpdateLogRecord(now_lsn, last_lsn, txid, page_id, offset, oldtext, input);
	logtail.push_back(upld);
	return now_lsn;
		
}

void LogMgr::setStorageEngine(StorageEngine* engine){
	se=engin;
}



