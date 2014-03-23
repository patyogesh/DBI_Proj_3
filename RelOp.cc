#include "RelOp.h"


int ctr = 1;
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
/* ======================================================================= */
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
/* ======================================================================= */
void *
Project_Worker(void *vptr)
{
  tParams_t *t_in_params = (tParams_t *) vptr;

  Record    temp;

  cin >> ctr;
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
//--------------- DuplicateRemoval ------------------

int DuplicateRemoval::m_runLen = 7;

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	 t_in_params = new (tParams_t);
	 t_in_params->inPipe = &inPipe;
	   t_in_params->outPipe = &outPipe;
	   t_in_params->pSchema = &mySchema;

    if (m_runLen == -1)
    {
        cerr << "\nError! Use_n_Page() must be called and "
            << "pages of memory allowed for operations must be set!\n";
        exit(1);
    }
    pthread_create(&thread, NULL, &DuplicateRemoval_Worker,
    		 (void *)t_in_params);
    return;
}

void * DuplicateRemoval::DuplicateRemoval_Worker(void * vptr)
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
    BigQ B(*(t_in_params->inPipe), localOutPipe, sortOrder,m_runLen);

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

void DuplicateRemoval::Use_n_Pages(int runlen)
{
	  this->runlen = runlen;
}

void DuplicateRemoval::WaitUntilDone()
{
    pthread_join(thread, 0);
}
//--------------- Sum ------------------

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	 t_in_params = new (tParams_t);
		 t_in_params->inPipe = &inPipe;
		   t_in_params->outPipe = &outPipe;
		   t_in_params->computeFunc = &computeMe;

    pthread_create(&thread, NULL, &Sum_Worker,
        (void*)t_in_params);

    return;
}

void * Sum::Sum_Worker(void * vptr)
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

void Sum::WaitUntilDone()
{
    pthread_join(thread, 0);
}
//--------------- GroupBy ------------------

void GroupBy::Use_n_Pages(int n)
{
    m_nRunLen = n;
}

void GroupBy::Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe)
{
    pthread_create(&m_thread, NULL, DoOperation, (void*)new Params(&inPipe, &outPipe, &groupAtts, &computeMe, m_nRunLen));
}

void GroupBy::WaitUntilDone()
{
    pthread_join(m_thread, 0);
}

void* GroupBy::DoOperation(void* p)
{
    Params* param = (Params*)p;
    //create a local outputPipe and a BigQ and an feed it with current inputPipe
    const int pipeSize = 100;
    Pipe localOutPipe(pipeSize);
    BigQ localBigQ(*(param->inputPipe), localOutPipe, *(param->groupAttributes), param->runLen);
    Record rec;
    Record *currentGroupRecord = new Record();
    bool currentGroupActive = false;
    ComparisonEngine ce;
    double sum = 0.0;
#ifdef _RELOP_DEBUG
    bool printed = false;
    int recordsInAGroup = 0;
    ofstream log_file("log_file");
    ofstream groupRecordLogFile("groupRecordLogFile");
#endif

    while(localOutPipe.Remove(&rec) || currentGroupActive)
    {
#ifdef _RELOP_DEBUG
        Schema suppSchema("catalog", "supplier");
        if(!printed)
        {
            cout<< "param->groupAttributes->numAtts" << param->groupAttributes->numAtts << endl;
            for (int i = 0; i < param->groupAttributes->numAtts; i++)
                cout<<"param->groupAttributes->whichAtts["<<i<<"] = "<<param->groupAttributes->whichAtts[i]<<endl;
            cout<<"columns in Record = "<<((int*)rec.bits)[1]/sizeof(int) - 1<<endl;
            printed = true;
        }
#endif

        if(!currentGroupActive)
        {
            currentGroupRecord->Copy(&rec);
            currentGroupActive = true;
#ifdef _RELOP_DEBUG
            rec.PrintToFile(&suppSchema, log_file);
            currentGroupRecord->PrintToFile(&suppSchema, groupRecordLogFile);
#endif
        }

        //either no new record fetched (end of pipe) or new group started so just go to else part and finish the last group
        if(rec.bits != NULL && ce.Compare(currentGroupRecord, &rec, param->groupAttributes) == 0)
        {
            int ival = 0; double dval = 0;
            param->computeMeFunction->Apply(rec, ival, dval);
            sum += (ival + dval);
            delete rec.bits;
            rec.bits = NULL;
#ifdef _RELOP_DEBUG
            recordsInAGroup++;
#endif
        }
        else
        {
#ifdef _RELOP_DEBUG
            cout<<"Records in a Group = "<<recordsInAGroup<<", and sum = "<<sum<<endl;
            //recordsInAGroup = 0;
#endif
            //store old sum and group-by attribtues concatenated in outputPipe
            //and also start new group from here

            // create temperory schema, with one attribute - sum
            Attribute att = {(char*)"sum", Double};
            Schema sum_schema((char*)"tmp_sum_schema_file", // filename, not used
                1, // number of attributes
                &att); // attribute pointer

            // Make a file that contains this sum
            string tempSumFileName = "tmp_sum_data_file" + System::getusec();
            FILE * sum_file = fopen(tempSumFileName.c_str(), "w");
            fprintf(sum_file, "%f|", sum);
            fclose(sum_file);
            sum_file = fopen(tempSumFileName.c_str(), "r");
            // Make record using the above schema and data file
            Record sumRec;
            sumRec.SuckNextRecord(&sum_schema, sum_file);
            fclose(sum_file);

#ifdef _RELOP_DEBUG
            //            cout<<"Sum Record : ";
            //            sumRec.Print(&sum_schema);
#endif

            //glue this record with the one we have from groupAttribute - not needed in q6
            int numAttsToKeep = param->groupAttributes->numAtts + 1;
            int attsToKeep[numAttsToKeep];
            attsToKeep[0] = 0;  //for sumRec
            for(int i = 1; i < numAttsToKeep; i++)
            {
                attsToKeep[i] = param->groupAttributes->whichAtts[i-1];
            }
            Record tuple;
            tuple.MergeRecords(&sumRec, currentGroupRecord, 1, numAttsToKeep - 1, attsToKeep,  numAttsToKeep, 1);
            // Push this record to outPipe

            param->outputPipe->Insert(&tuple);

            //initialize sum from last unused record (if any)
            if(rec.bits != NULL)
            {
                int ival = 0; double dval = 0;
                param->computeMeFunction->Apply(rec, ival, dval);
                sum = (ival + dval);    //not += here coz we are re-initializing sum
                delete rec.bits;
                rec.bits = NULL;
            }

            //start new group for this record
            currentGroupActive = false;
            // delete file "tmp_sum_data_file"
            if(remove(tempSumFileName.c_str()) != 0)
                perror("\nError in removing tmp_sum_data_file!");
        }
    }

#ifdef _RELOP_DEBUG
    log_file.flush();
    log_file.close();
    groupRecordLogFile.flush();
    groupRecordLogFile.close();
    cout<<"sum in last group (after finish) = "<< sum<<endl;
    cout<<"recs in last group (after finish) = "<< recordsInAGroup<<endl;
#endif

    // Shut down the outpipe
    param->outputPipe->ShutDown();
    delete param;
    param = NULL;
}

