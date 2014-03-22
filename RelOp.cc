#include "RelOp.h"


int cntr = 1;
void *
SelectFile_Worker(void *vptr) 
{
  tParams_t *t_in_params = (tParams_t *) vptr;
  Record    temp;

  t_in_params->dbFile->MoveFirst();
  while(t_in_params->dbFile->GetNext(temp,
                               *(t_in_params->cnf),
                               *(t_in_params->lit))) {
    t_in_params->outPipe->Insert(&temp);
  }
  t_in_params->outPipe->ShutDown();
}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
  t_in_params = new (tParams_t);
  cout <<"\n\n I m in "<< __FUNCTION__;

  memset(t_in_params, 0x00, sizeof(tParams_t));
  t_in_params->dbFile = &inFile;
  t_in_params->inPipe = NULL;
  t_in_params->outPipe = &outPipe;
  t_in_params->cnf = &selOp;
  t_in_params->lit = &literal;

  if(pthread_create(&thread,
                    NULL,
                    SelectFile_Worker,
                    (void *)t_in_params)) {
    perror("Error Creating worker thread for SelectFile operation");
    exit(1);
  }

}

void SelectFile::WaitUntilDone () 
{
	pthread_join (thread, NULL);

  if(t_in_params) {
    delete t_in_params;
    t_in_params = NULL;
  }
}

void SelectFile::Use_n_Pages (int runlen) {
  this->runlen = runlen;
}
