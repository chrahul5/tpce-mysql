// EGenSimpleTest.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"
#include <vector>

using namespace std;
using namespace TPCE;

extern int MEESUT_ThreadCount; //MEESUT.cpp

char szInDir[iMaxPath] = "flat_in";
char szHost[iMaxHostname] = "localhost";
char szDBName[iMaxDBName] = "tpce";
char szDBUser[iMaxDBName] = "tpce";
char szDBPass[iMaxDBName] = "tpce";
TIdent iConfiguredCustomerCount = iDefaultCustomerCount;
TIdent iActiveCustomerCount = iDefaultCustomerCount;
int iScaleFactor = 500;
int iDaysOfInitialTrades = 300;
int iLoadUnitSize = iDefaultLoadUnitSize;
int iTestDuration = 0;
int iRampupDuration = 0;
int iSleep = 1000; // msec between thread creation
int iUsers = 0; // # users
int iThreads = 1; // # users
int iPacingDelay = 0; //10?
char outputDirectory[iMaxPath] = "."; // path to output files

CLogFormatTab g_fmt;
#ifndef DEBUG
CEGenNullLogger* g_pLog;
#else
CEGenLogger* g_pLog;
#endif

eGlobalState GlobalState = INITIAL;
CCETxnInputGenerator* m_TxnInputGenerator;

// CRtHistogram g_RtHistogram[TRADE_CLEANUP]; //Hmm.. it isn't clean...

/////////////////////////// Inialize fields ///////////////////////////////////

CInputFiles						inputFiles;

CCETxnInputGenerator*  transactions_input_init(int customers, int sf, int wdays) 
{	
//	TDriverCETxnSettings		m_DriverCETxnSettings;

		// Create log formatter and logger instance
	CLogFormatTab * fmt= new CLogFormatTab();
	
	CEGenLogger* log = new CEGenLogger(eDriverCE, 0, "EGenTrxInput.log", fmt);
	
	PDriverCETxnSettings mDriverCETxnSettings = new TDriverCETxnSettings();
	INT32 sf1 = sf;
	INT32 wdays1 = wdays;

	// Inialize input files.
	cout << "initializing input generator " << iConfiguredCustomerCount << ' ' << iActiveCustomerCount << ' ' << szInDir << endl;
	inputFiles.Initialize(eDriverCE, iConfiguredCustomerCount,
			    iActiveCustomerCount, szInDir);
	cout << "input files initialized" << endl;
	// assert(inputFiles!=NULL);
	CCETxnInputGenerator*  m_TxnInputGenerator = new CCETxnInputGenerator(inputFiles,
				(TIdent)customers, (TIdent)customers, sf1,
				wdays1 * HoursPerWorkDay, log, mDriverCETxnSettings);
	return m_TxnInputGenerator;
}

unsigned int AutoRand() {
  struct timeval tv;
  struct tm ltr;
  gettimeofday(&tv, NULL);
  struct tm *lt = localtime_r(&tv.tv_sec, &ltr);
  return (((lt->tm_hour * MinutesPerHour + lt->tm_min) * SecondsPerMinute +
           lt->tm_sec) *
              MsPerSecond +
          tv.tv_usec / 1000);
}

CMEE* market_init(INT32 TradingTimeSoFar, CMEESUTInterface *pSUT, UINT32 UniqueId){
	
     // Create log formatter and logger instance
		CLogFormatTab * fmt= new CLogFormatTab();
		CEGenLogger* log = new CEGenLogger(eDriverCE, 0, "MarketInput.log", fmt);

	CMEE* mee= new CMEE(TradingTimeSoFar, pSUT,  log, inputFiles, UniqueId);
	return mee;
}

//Alarm
int timer_count = 0;
//PRINT_INTERVAL must be divisor of 60
#define PRINT_INTERVAL 10

//////////////////// Code utils ////////////////////////////////

void push_message(PTradeRequest pMessage)
{
	// Obsolete.
}


// Prints program usage to std error.
void Usage()
{
    cerr <<
	"\nUsage: EGenSimpleTest {options}" << endl << endl <<
	"  where" << endl <<
	"   Option      Default                Description" << endl <<
	"   =========   ===================    =============================================" << endl <<
	"   -e string   " << szInDir  << "\t\t      Path to EGen input files" << endl <<
#ifdef ODBC_WRAPPER
	"   -S string   " << szHost   << "\t      Database server" << endl <<
	"   -D string   " << szDBName << "\t\t      Database name" << endl <<
#else
	"   -D string   " << szDBName << "\t\t      Data source name" << endl <<
#endif
	"   -U string   " << szDBUser << "\t\t      Database user" << endl <<
	"   -P string   " << szDBPass << "\t\t      Database password" << endl <<
	"   -c number   " << iConfiguredCustomerCount << "\t\t      Configured customer count" << endl <<
	"   -a number   " << iActiveCustomerCount << "\t\t      Active customer count" << endl <<
	"   -f number   " << iScaleFactor << "\t\t      # of customers for 1 TRTPS" << endl <<
	"   -d number   " << iDaysOfInitialTrades << "\t\t      # of Days of Initial Trades" << endl <<
	"   -l number   " << iLoadUnitSize << "\t\t      # of customers in one load unit" << endl <<
	"   -t number                          Duration of the test (seconds)" << endl <<
	"   -r number                          Duration of ramp up period (seconds)" << endl <<
	"   -u number                          # of Users" << endl
#ifdef DEBUG
	 << "   -o string   " << outputDirectory << "\t\t      # directory for output files" << endl
#endif
	;
}

// Parses input command.
void ParseCommandLine( int argc, char *argv[] )
{
    int   arg;
    char  *sp;
    char  *vp;

    /*
     *  Scan the command line arguments
     */
    for ( arg = 1; arg < argc; ++arg ) {

	/*
	 *  Look for a switch
	 */
	sp = argv[arg];
	if ( *sp == '-' ) {
	    ++sp;
	}

	/*
	 *  Find the switch's argument.  It is either immediately after the
	 *  switch or in the next argv
	 */
	vp = sp + 1;
	// Allow for switched that don't have any parameters.
	// Need to check that the next argument is in fact a parameter
	// and not the next switch that starts with '-'.
	//
	if ( (*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-') )
	{
	    vp = argv[++arg];
	}

	/*
	 *  Parse the switch
	 */
	switch ( *sp ) {
	    case 'e':       // input files path
		strncpy(szInDir, vp, sizeof(szInDir));
		break;
	    case 'S':       // Database host name.
#ifdef ODBC_WRAPPER
		strncpy(szHost, vp, sizeof(szHost));
		break;
#endif
	    case 'D':       // Database name.
		strncpy(szDBName, vp, sizeof(szDBName));
		break;
	    case 'U':       // Database user.
		strncpy(szDBUser, vp, sizeof(szDBName));
		break;
	    case 'P':       // Database password.
		strncpy(szDBPass, vp, sizeof(szDBName));
		break;
	    case 'c':
		sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
		break;
	    case 'a':
		sscanf(vp, "%"PRId64, &iActiveCustomerCount);
		break;
	    case 'f':
		sscanf(vp, "%d", &iScaleFactor);
		break;
	    case 'd':
		sscanf(vp, "%d", &iDaysOfInitialTrades);
		break;
	    case 'l':
		sscanf(vp, "%d", &iLoadUnitSize);
		break;
	    case 't':
		sscanf(vp, "%d", &iTestDuration);
		break;
	    case 'r':
		sscanf(vp, "%d", &iRampupDuration);
		break;
	    case 's':
		sscanf(vp, "%d", &iSleep);
		break;
	    case 'u':
		sscanf(vp, "%d", &iUsers);
		break;
		case 'T':
		sscanf(vp, "%d", &iThreads);
		break;
	    case 'p':
		sscanf(vp, "%d", &iPacingDelay);
		break;
	    case 'o':
		strncpy(outputDirectory, vp,
			sizeof(outputDirectory));
		break;
	    default:
		Usage();
		fprintf( stderr, "Error: Unrecognized option: %s\n",sp);
		exit( ERROR_BAD_OPTION );
	}
    }

}

bool ValidateParameters()
{
    bool bRet = true;

    //TODO:

    return bRet;
}

//////////////////// Transaction implementation ////////////////////

std::vector<CMEE *> mees;
std::vector<MFBuffer *> MarketFeedInputBuffers;
std::vector<TRBuffer *> TradeResultInputBuffers;

// resp for [partition_id_start, partition_id_end)
tpce_worker::tpce_worker(CDBConnection *pDBConn, unsigned int worker_id, 
	CMEE * mee, MFBuffer * MarketFeedInputBuffer,TRBuffer * TradeResultInputBuffer):  CTxnDBBase(pDBConn), worker_id(worker_id) {
	auto i = worker_id % iThreads;
	mee = mees[i];
	MarketFeedInputBuffer = MarketFeedInputBuffers[i];
	TradeResultInputBuffer = TradeResultInputBuffers[i];
}

// BrokerVolume transaction.
void tpce_worker::broker_volume() {
  TBrokerVolumeTxnInput input;
  TBrokerVolumeTxnOutput output;
  m_TxnInputGenerator->GenerateBrokerVolumeInput(input);
  CBrokerVolume *harness = new CBrokerVolume(this);

  harness->DoTxn((PBrokerVolumeTxnInput)&input,
                                  (PBrokerVolumeTxnOutput)&output);
}

// Customer Position transaction.
void tpce_worker::customer_position() {
    TCustomerPositionTxnInput input;
    TCustomerPositionTxnOutput output;
    m_TxnInputGenerator->GenerateCustomerPositionInput(input);
    CCustomerPosition *harness = new CCustomerPosition(this);

    harness->DoTxn((PCustomerPositionTxnInput)&input,
                                   (PCustomerPositionTxnOutput)&output);
}

// Security Detail transaction.
void tpce_worker::security_detail() {
    TSecurityDetailTxnInput input;
    TSecurityDetailTxnOutput output;
    m_TxnInputGenerator->GenerateSecurityDetailInput(input);
    CSecurityDetail *harness = new CSecurityDetail(this);

    harness->DoTxn((PSecurityDetailTxnInput)&input,
                                   (PSecurityDetailTxnOutput)&output);
}




//////////////////// Worker threads ////////////////////////////////

vector<tpce_worker> benchmark_workers;

// Create upto iThread number of TPCE workers
void make_workers() {
	// Create different connections for each thread.
	CDBConnection* m_pDBConnection = new CDBConnection(szHost, szDBName,
					szDBUser, szDBPass, 1);
	for(unsigned int ii = 0; ii < iThreads; ii++) {
		auto mf_buf = new MFBuffer();
		auto tr_buf = new TRBuffer();
		MarketFeedInputBuffers.emplace_back(mf_buf);
		TradeResultInputBuffers.emplace_back(tr_buf);
		auto meesut = new myMEESUT();
		meesut->setMFQueue(mf_buf);
		meesut->setTRQueue(tr_buf);
		auto mee = market_init(iDaysOfInitialTrades * 8, meesut, AutoRand());
		mees.emplace_back(mee);
		// cout << "length of stuff " << ii << " " << mees.size() << " " << MarketFeedInputBuffers.size() << " " << TradeResultInputBuffers.size() << endl;
		
		benchmark_workers.emplace_back(
            tpce_worker(m_pDBConnection, ii, mees[ii],
            MarketFeedInputBuffers[ii], TradeResultInputBuffers[ii]));
	}
}

void run_benchmark() {
	for(auto w: benchmark_workers) {
		w.run_test();
	}
}

int main(int argc, char* argv[]) {
    long UniqueID = 0;
    pthread_t *t;
    struct itimerval itval;
    struct sigaction  sigact;

    // Output EGen version
    PrintEGenVersion();

#ifdef MYSQL_ODBC
    cout << "(for MySQL)" << endl;
#elif PGSQL_ODBC
    cout << "(for PosrgreSQL)" << endl;
#elif ORACLE_ODBC
    cout << "(for Oracle)" << endl;
#else
    cout << "(for ?)" << endl;
#endif

#ifdef USE_PREPARE
    cout << "(Prepared Statement)" << endl;
#else
    cout << "(Literal SQL)" << endl;
#endif

    // Parse command line
    ParseCommandLine(argc, argv);

    // Validate parameters
    if (!ValidateParameters())
    {
	return ERROR_INVALID_OPTION_VALUE;      // exit returning a non-zero code
    }

    cout<<endl<<"Using the following settings:"<<endl<<endl;
    cout<<"\tInput files location:\t\t"<<     szInDir <<endl;
#ifdef ODBC_WRAPPER
    cout<<"\tDatabase server:\t\t"<<          szHost <<endl;
    cout<<"\tDatabase name:\t\t\t"<<          szDBName <<endl;
#else
    cout<<"\tData source name:\t\t"<<       szDBName <<endl;
#endif
    cout<<"\tDatabase user:\t\t\t"<<          szDBUser <<endl;
    cout<<"\tDatabase password:\t\t"<<        szDBPass <<endl;
    cout<<"\tConfigured customer count:\t"<<  iConfiguredCustomerCount <<endl;
	cout<<"\tNumber of threads:\t\t"<<    	  iThreads <<endl;
    cout<<"\tActive customer count:\t\t"<<    iActiveCustomerCount <<endl;
    cout<<"\tScale Factor:\t\t\t"<<           iScaleFactor <<endl;
    cout<<"\t#Days of initial trades:\t"<<    iDaysOfInitialTrades <<endl;
    cout<<"\tLoad unit size:\t\t\t"<<         iLoadUnitSize <<endl;
    cout<<"\tTest duration (sec):\t\t"<<      iTestDuration <<endl;
    cout<<"\tRamp up duration (sec):\t\t"<<   iRampupDuration <<endl;
    cout<<"\t# of Users:\t\t\t"<<             iUsers <<endl;
#ifdef DEBUG
    cout<<"\tDirectory for output files:\t"<< outputDirectory <<endl<<endl;
#endif

	cout << "hello world" << endl;

	// Initialize Client Transaction Input generator
 	m_TxnInputGenerator = transactions_input_init(iConfiguredCustomerCount, iScaleFactor, iDaysOfInitialTrades);
	cout << "input generator initialized" << endl;

	// Initialize workers.
	make_workers();
	cout << "initialized all workers" << endl;

	run_benchmark();

	// Initalize
    // pthread_exit(NULL); //exit with waiting detached threads.
}