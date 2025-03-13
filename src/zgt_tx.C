/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2025: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>


extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort_operation(long, char);   //commit/abort based on char value
extern void *process_read_write_operation(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus, char type, pthread_t thrid){
  this->lockmode = (char)' ';   // default
  this->Txtype = type;          // R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1;              // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1;             // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. When creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    // Writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%d\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  struct param *node = (struct param*)arg;// get tid and objno and count

  // do the operations for reading. Write your code
  long tid = node->tid;
  long obno = node->obno;
  long count = node->count;
  process_read_write_operation(tid,obno,count,'R');
  pthread_exit(NULL);

    
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
  struct param *node = (struct param*)arg;	// struct parameter that contains
  
  // do the operations for writing; similar to readTx. Write your code
  long tid = node->tid;
  long obno = node->obno;
  long count = node->count;
  process_read_write_operation(tid,obno,count,'W');
  pthread_exit(NULL);
}

// common method to process read/write: Just a suggestion

void *process_read_write_operation(long tid, long obno,  int count, char mode){

  // Start operation
  start_operation(tid,count);
  #ifdef TX_DEBUG
    printf("\n Performing %stx: for %d for obj %d", (mode == 'R')? "Read": "Write",tid,obno);
    fflush(stdout);
  #endif

  zgt_tx *tx = get_tx(tid);

  // see if the tx exist
  if(tx == NULL)
  {
    printf("\n Error: Transaction %d not found", tid);
    fflush(stdout);
    zgt_v(tx->semno); // release the resource
    pthread_exit(NULL);
  }
  // if it does exsit then then lock the tm
  zgt_p(0);

  #ifdef TX_DEBUG
      printf("\n %stx: got the lock for %d ", (mode == 'R')? "Read": "Write",tid);
      fflush(stdout);
    #endif
  // check if the tx has been aborted
  if(tx->get_status() == TR_ABORT)
  {
    zgt_v(0);
    finish_operation(tid);
    pthread_exit(NULL); 
  }

  // see what lock it is
  char lock_mode = (mode =='R') ? 'S':'X';

  // get the lock status
  int lock_status = tx->set_lock(tid,tx->sgno,obno,count,lock_mode);

  if(lock_status == 1)
  {
    zgt_v(0); // realase the tm
    zgt_p(tx->semno); // wait for the semaphore
    
    zgt_p(0); // get the tm again

    if(tx->get_status() == TR_ABORT)
    {
      zgt_v(0);
      finish_operation(tid);
      pthread_exit(NULL);
    }
  }
  
  // read/write logic

  if(mode == 'R')
  {
    ZGT_Sh->objarray[obno]->value -= 4;
  }
  else
  {
    ZGT_Sh->objarray[obno]->value += 7;
  }


  // log the operation

  fprintf(ZGT_Sh->logfile,"T%d\t %c\t %s\t %d:%d:%d %sLock\t Granted\t %c \n",
    tid,
    ZGT_Sh->lastr->Txtype,
    (mode == 'R')? "Readtx":"Wrtietx",
    obno,
    ZGT_Sh->objarray[obno]->value,
    ZGT_Sh->optime[tid],
    (mode == 'R')? "Read":"Wrtie",
    tx->get_status());
  fflush(ZGT_Sh->logfile);
  // release tm lock
  zgt_v(0);
  // sleep till tx is done
  usleep(ZGT_Sh->optime[tid]);
    #ifdef TX_DEBUG
      printf("\n %stx: %d completed for obj %d", (mode == 'R')? "Read": "Write",tid,obno);
      fflush(stdout);
    #endif

  finish_operation(tid);
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;// get tid and count  
  long tid = node->tid;
  long count = node->count;
  //write your code
  // should release all the locks, doesnt have to me in a particiular order
  start_operation(tid,count);
  do_commit_abort_operation(tid,TR_ABORT); // call command with parameter for "abort"
  pthread_exit(NULL);			// thread exit
}

void *committx(void *arg)
{
 
    //remove the locks/objects before committing

  struct param *node = (struct param*)arg;// get tid and count
  long tid = node->tid;
  long count = node->count;

  //write your code

  start_operation(tid,count);
  
  do_commit_abort_operation(tid,TR_END); // call command with parameter for "comit"

  pthread_exit(NULL);			// thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existent tx

void *do_commit_abort_operation(long t, char status){

  // write your code
  zgt_tx *tx = get_tx(t);
  if(tx == NULL)
  {
    fprintf(ZGT_Sh->logfile, "ERROR: Trying to %s a non-existent transaction: %ld\n", (status == TR_END) ? "commit" : "abort", t);
    fflush(ZGT_Sh->logfile);
    pthread_exit(NULL);
  }
  if(status == TR_END)
  {
    // Free blocks before commiting
    tx->free_locks();

    // log the commit operation
    fprintf(ZGT_Sh->logfile, "T%ld\tCommitTx\n", t);
    fflush(ZGT_Sh->logfile);
    } 
    else 
    { 
      // if (status == TR_ABORT)
      // Log the abort operation
      fprintf(ZGT_Sh->logfile, "T%ld\tAbortTx\n", t);
      fflush(ZGT_Sh->logfile);

      // Free locks after aborting
      tx->free_locks();
  }
  // Realease waiting transactions
  if(tx->semno != -1)
  {
    int waiting_tx_count = zgt_nwait(tx->semno);
    for(int i =0;i<waiting_tx_count;i++)
    {
      zgt_v(tx->semno);
    }
  }

  // Remove the transaction from the transaction manager
  tx->remove_tx();

  // finish operation and exit tread
  finish_operation(t);
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1
  
  //write your code
  // should be similar to the free_locks below
  //semophore =0 means that its locked
  //optime is when you should sleep an operation
  zgt_hlink *resource = ZGT_Ht->find(tid1,obno1);
  if (resource != NULL)
  {
    #ifdef TX_DEBUG
      printf("\nset_lock %d already holds lock on obj, %d", tid,obno1);
      fflush(stdout);
    #endif
    return 0;
  }

  // check for Conflicts with other txs
  zgt_hlink *conflict = ZGT_Ht->find(sgno1,obno1);

  if(conflict != NULL && conflict->tid != tid1)
  {
    if(conflict->lockmode == 'X' ||(lockmode1 == 'X' && conflict->lockmode == 'S'))
    {
      this->status = TR_WAIT; // sets the status to wait
      this->lockmode = lockmode1;
      this->obno = obno1;

      long conflict_tid = conflict->tid;
      this->setTx_semno(conflict_tid,tid1);
      #ifdef TX_DEBUG
        printf("\nset_lock %d waiting for object %d held by %d",tid,obno1,conflict_tid);
        fflush(stdout);
      #endif 
      return 1; // lock not gotten, needs to wait

    } 
  }
  if(ZGT_Ht->add(this,sgno1,obno1,lockmode1) == -1)
  {
    printf("::: ERROR: Not enough memory to store obno:%d in lock hash table for node with tid: %d", obno1,tid1);
    fflush(stdout);
    return -1; // error for the getting the lcok
  }
  #ifdef TX_DEBUG
    printf("\nset_lock %d got %c lcok on obj %d", tid1,lockmode1,obno1);
    fflush(stdout);
  #endif
  return 0;
}

int zgt_tx::free_locks()
{
  
  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink* temp = head;  //first obj of tx
  
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(ZGT_Sh->logfile, "%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(ZGT_Sh->logfile);
      
      if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
	   printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
	   fflush(stdout);
      }
      else {
#ifdef TX_DEBUG
	   printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                            temp->tid, temp->obno, temp->lockmode);
	   fflush(stdout);
#endif
      }
    }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the actual read/write operation as described the project description
// based  on the lockmode

void zgt_tx::perform_read_write_operation(long tid,long obno, char lockmode){
  
  // write your code

}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}


