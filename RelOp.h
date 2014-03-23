#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <string.h>
#include <iostream>

typedef struct Thread_aparams {
  DBFile  *dbFile;
  Pipe    *inPipe;
  Pipe    *outPipe;
  CNF     *cnf;
  Record  *lit;
  Schema *pSchema;
  Function *computeFunc;

  int     *atts_to_keep;
  int     num_atts_in;
  int     num_atts_out;
}tParams_t;

class RelationalOp {
  protected:
  int          runlen;
  tParams_t    *t_in_params;
	public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

	private:
	pthread_t   thread;
	Record *buffer;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
	private:
	pthread_t   thread;

  public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Project : public RelationalOp {
	private:
	pthread_t   thread;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class Join : public RelationalOp {
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
private:
	pthread_t   thread;
	static int m_runLen;

	void* DuplicateRemoval_Worker(void*);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class Sum : public RelationalOp {
private:
	pthread_t   thread;

	 static void* Sum_Worker(void*);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
#endif
