#include "RelOp.h"
#include <sys/time.h>
#include <sstream>

int ctr = 1;
/* =========================SelectFile============================================== */
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
/* =======================SelectPipe================================================ */
void *
SelectPipe_Worker(void *vptr)
{
  tParams_t *t_in_params = (tParams_t *) vptr;
}
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
  t_in_params = new (tParams_t);
  cout <<"\n\n I m in "<< __FUNCTION__;

  memset(t_in_params, 0x00, sizeof(tParams_t));
  t_in_params->dbFile = NULL;
  t_in_params->inPipe = &inPipe;
  t_in_params->outPipe = &outPipe;
  t_in_params->cnf = &selOp;
  t_in_params->lit = &literal;

  if(pthread_create(&thread,
                    NULL,
                    SelectPipe_Worker,
                    (void *)t_in_params)) {
    perror("Error Creating worker thread for SelectFile operation");
    exit(1);
  }
}

void SelectPipe::WaitUntilDone ()
{
	pthread_join (thread, NULL);

  if(t_in_params) {
    delete t_in_params;
    t_in_params = NULL;
  }
}

void SelectPipe::Use_n_Pages (int runlen) {
  this->runlen = runlen;
}
/* ==========================Project============================================= */
void *
Project_Worker(void *vptr)
{
  tParams_t *t_in_params = (tParams_t *) vptr;

  Record    temp;

  ctr = 0;
  while(t_in_params->inPipe->Remove(&temp)) {

    temp.Project(t_in_params->atts_to_keep,
        t_in_params->num_atts_out,
        t_in_params->num_atts_in);

    t_in_params->outPipe->Insert(&temp);
    cout <<"\n\nInserted To Pipe : "<<++ctr;
  }
  t_in_params->inPipe->ShutDown();
  t_in_params->outPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
  t_in_params = new (tParams_t);
  cout <<"\n\n I m in "<< __FUNCTION__;

  memset(t_in_params, 0x00, sizeof(tParams_t));
  t_in_params->inPipe = &inPipe;
  t_in_params->outPipe = &outPipe;
  t_in_params->atts_to_keep = keepMe;
  t_in_params->num_atts_in = numAttsInput;
  t_in_params->num_atts_out = numAttsOutput;

  if(pthread_create(&thread,
                    NULL,
                    Project_Worker,
                    (void *)t_in_params)) {
    perror("Error Creating worker thread for Project operation");
    exit(1);
  }
}

void Project::WaitUntilDone ()
{
	pthread_join (thread, NULL);

  if(t_in_params) {
    delete t_in_params;
    t_in_params = NULL;
  }
}

void Project::Use_n_Pages (int runlen) {
  this->runlen = runlen;
}
/* ======================DuplicateRemoval====================================== */

//int DuplicateRemoval::m_runLen = 7;

void* DuplicateRemoval_Worker(void * vptr)
{
	 tParams_t *t_in_params = (tParams_t *) vptr;
    Record lastSeenRec, currentRec;
    OrderMaker sortOrder;
    int pipeSize = 100;

    int n = t_in_params->pSchema->GetNumAtts();
    Attribute *atts = t_in_params->pSchema->GetAtts();
    sortOrder.numAtts = n;
    for (int i = 0; i < n; i++)
    {
        sortOrder.whichAtts[i] = i;
        sortOrder.whichTypes[i] = atts[i].myType;
    }

     Pipe localOutPipe(pipeSize);

     BigQ bigQ(*t_in_params->inPipe, localOutPipe, sortOrder, t_in_params->runlen);
//    BigQ B(*(t_in_params->inPipe), localOutPipe, sortOrder,m_runLen);

    bool bLastSeenRecSet = false;
    ComparisonEngine ce;
    while (localOutPipe.Remove(&currentRec))
    {
        if (bLastSeenRecSet == false)
        {
            lastSeenRec.Copy(&currentRec);
            bLastSeenRecSet = true;
        }


        if (ce.Compare(&lastSeenRec, &currentRec, &sortOrder) != 0)
        {
        	t_in_params->outPipe->Insert(&lastSeenRec);
            lastSeenRec.Copy(&currentRec);
        }
    }

    t_in_params->outPipe->Insert(&lastSeenRec);

    localOutPipe.ShutDown();
    t_in_params->outPipe->ShutDown();

    delete t_in_params;
    t_in_params = NULL;
}
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
  t_in_params = new (tParams_t);
  t_in_params->inPipe = &inPipe;
  t_in_params->outPipe = &outPipe;
  t_in_params->pSchema = &mySchema;
  t_in_params->runlen = runlen;

  if (runlen == -1)
  {
    cerr << "\nError! Use_n_Page() must be called and "
      << "pages of memory allowed for operations must be set!\n";
    exit(1);
  }
  pthread_create(&thread, NULL, DuplicateRemoval_Worker,
      (void *)t_in_params);
  // return;
}


void DuplicateRemoval::Use_n_Pages(int runlen)
{
	  this->runlen = runlen;
}

void DuplicateRemoval::WaitUntilDone()
{
    pthread_join(thread, 0);
}
/* ========================Sum======================================= */

void* Sum_Worker(void * vptr)
{
	 tParams_t *t_in_params = (tParams_t *) vptr;
    Record rec;
    double sum = 0;

    while(t_in_params->inPipe->Remove(&rec))
    {
        int ival = 0; double dval = 0;
        t_in_params->computeFunc->Apply(rec, ival, dval);
        sum += (ival + dval);
    }

    Attribute att = {(char*)"sum", Double};

    Schema sum_schema((char*)"temp_sum_schema",1, &att);

    FILE * sum_file = fopen("temp_sum_data", "w");
    fprintf(sum_file, "%f|", sum);
    fclose(sum_file);
    sum_file = fopen("temp_sum_data", "r");
    rec.SuckNextRecord(&sum_schema, sum_file);
    fclose(sum_file);

    t_in_params->outPipe->Insert(&rec);

    t_in_params->outPipe->ShutDown();

    if(remove("temp_sum_data") != 0)

    delete t_in_params;
    t_in_params = NULL;
}
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
  t_in_params = new (tParams_t);
  t_in_params->inPipe = &inPipe;
  t_in_params->outPipe = &outPipe;
  t_in_params->computeFunc = &computeMe;
  t_in_params->runlen = runlen;

  pthread_create(&thread, NULL, Sum_Worker,
      (void*)t_in_params);

  return;
}


void Sum::Use_n_Pages(int runlen)
{
	this->runlen = runlen;
}
void Sum::WaitUntilDone()
{
    pthread_join(thread, 0);
}
/* ========================GroupBy================================== */

void GroupBy::Use_n_Pages(int runlen)
{
	this->runlen = runlen;
}

void* GroupBy_Worker(void* vptr)
{
	tParams_t *t_in_params = (tParams_t *) vptr;
    const int pipeSize = 100;
    Pipe localOutPipe(pipeSize);
    BigQ localBigQ(*(t_in_params->inPipe), localOutPipe, *(t_in_params->groupAttributesOM), t_in_params->runlen);
    Record rec;
    Record *currentGroupRecord = new Record();
    bool currGroupFlag = false;
    ComparisonEngine comp;
    double sum = 0.0;


    while(localOutPipe.Remove(&rec) || currGroupFlag)
    {
        if(!currGroupFlag)
        {
            currentGroupRecord->Copy(&rec);
            currGroupFlag = true;
        }

        //either no new record fetched (end of pipe) or new group started so just go to else part and finish the last group
        if(rec.bits != NULL && comp.Compare(currentGroupRecord, &rec, t_in_params->groupAttributesOM) == 0)
        {
            int ival = 0; double dval = 0;
            t_in_params->computeFunc->Apply(rec, ival, dval);
            sum += (ival + dval);
            delete rec.bits;
            rec.bits = NULL;
        }
        else
        {
            Attribute att = {(char*)"sum", Double};
            Schema sum_schema((char*)"tmp_sum_schema_file", // filename, not used
                1, // number of attributes
                &att); // attribute pointer


        	struct timeval tval;
        		gettimeofday(&tval, NULL);
        		stringstream ss;
        		ss << tval.tv_sec;
        		ss << ".";
        		ss << tval.tv_usec;

        //		string filename = "partial" + ss.str();

//        	g_filePath = strdup(filename.c_str());

            // Make a file that contains this sum
            string tempSumFName = "temp_sum_data" + ss.str();
            FILE * sum_file = fopen(tempSumFName.c_str(), "w");
            fprintf(sum_file, "%f|", sum);
            fclose(sum_file);
            sum_file = fopen(tempSumFName.c_str(), "r");
            // Make record using the above schema and data file
            Record sumRec;
            sumRec.SuckNextRecord(&sum_schema, sum_file);
            fclose(sum_file);


            //glue this record with the one we have from groupAttribute - not needed in q6
            int numAttsToKeep = t_in_params->groupAttributesOM->numAtts + 1;
            int attsToKeep[numAttsToKeep];
            attsToKeep[0] = 0;  //for sumRec
            for(int i = 1; i < numAttsToKeep; i++)
            {
                attsToKeep[i] = t_in_params->groupAttributesOM->whichAtts[i-1];
            }
            Record tuple;
            tuple.MergeRecords(&sumRec, currentGroupRecord, 1, numAttsToKeep - 1, attsToKeep,  numAttsToKeep, 1);

            t_in_params->outPipe->Insert(&tuple);

            if(rec.bits != NULL)
            {
                int ival = 0; double dval = 0;
                t_in_params->computeFunc->Apply(rec, ival, dval);
                sum = (ival + dval);    //not += here coz we are re-initializing sum
                delete rec.bits;
                rec.bits = NULL;
            }

            currGroupFlag = false;
            if(remove(tempSumFName.c_str()) != 0)
                perror("\nError in removing tmp_sum_data_file!");
        }
    }

    // Shut down the outpipe
    t_in_params->outPipe->ShutDown();
    delete t_in_params;
    t_in_params = NULL;
}
void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe)
{

  t_in_params = new (tParams_t);
  t_in_params->inPipe = &inPipe;
  t_in_params->outPipe = &outPipe;
  t_in_params->computeFunc = &computeMe;
  t_in_params->groupAttributesOM = &groupAtts;
  t_in_params->runlen = runlen;

  pthread_create(&thread, NULL, GroupBy_Worker, (void*)t_in_params);
}

void GroupBy::WaitUntilDone()
{
    pthread_join(thread, 0);
}


