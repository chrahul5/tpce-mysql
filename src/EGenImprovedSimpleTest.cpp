// EGenSimpleTest.cpp
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"
#include <vector>

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
int iThreads = 0; // # users
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

unsigned int g_TxnCount[TRADE_CLEANUP][4]; //TRADE_CLEANUP isn't counted

CRtHistogram g_RtHistogram[TRADE_CLEANUP]; //Hmm.. it isn't clean...

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

//////////////////// Worker threads ////////////////////////////////

// Create upto iThread number of TPCE workers
void* make_workers() {

}

std::vector<CMEE *> mees;
std::vector<MFBuffer *> MarketFeedInputBuffers;
std::vector<TRBuffer *> TradeResultInputBuffers;

class tpce_worker : public CTxnDBBase,
					public CBrokerVolumeDBInterface,
                    public CCustomerPositionDBInterface,
                    public CMarketFeedDBInterface,
                    public CMarketWatchDBInterface,
                    public CSecurityDetailDBInterface,
                    public CTradeLookupDBInterface,
                    public CTradeOrderDBInterface,
                    public CTradeResultDBInterface,
                    public CTradeStatusDBInterface,
                    public CTradeUpdateDBInterface,
                    public CDataMaintenanceDBInterface,
                    public CTradeCleanupDBInterface,
                    public CSendToMarketInterface {
	public:
  // resp for [partition_id_start, partition_id_end)
  tpce_worker(CDBConnection *pDBConn, unsigned int worker_id):  CTxnDBBase(pDBConn), worker_id(worker_id) {
    auto i = worker_id % iThreads;
    mee = mees[i];
    // ALWAYS_ASSERT(i >= 0 and i < mees.size());
    MarketFeedInputBuffer = MarketFeedInputBuffers[i];
    TradeResultInputBuffer = TradeResultInputBuffers[i];
    // ALWAYS_ASSERT(TradeResultInputBuffer and MarketFeedInputBuffer and mee);
  }

  // Market Interface
  bool SendToMarket(TTradeRequest &trade_mes) {
    mee->SubmitTradeRequest(&trade_mes);
    return true;
  }

  // Broker volume interface.
  void broker_volume() {
    TBrokerVolumeTxnInput input;
    TBrokerVolumeTxnOutput output;
    m_TxnInputGenerator->GenerateBrokerVolumeInput(input);
    CBrokerVolume *harness = new CBrokerVolume(this);

    harness->DoTxn((PBrokerVolumeTxnInput)&input,
                                   (PBrokerVolumeTxnOutput)&output);
  }
  void DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
					   TBrokerVolumeFrame1Output *pOut);

  private:
  uint worker_id;
  CMEE *mee;  // thread-local MEE
  MFBuffer *MarketFeedInputBuffer;
  TRBuffer *TradeResultInputBuffer;
};

int main(int argc, char* argv[])
{
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

    /* Set up Logger */
#ifndef DEBUG
    g_pLog = new CEGenNullLogger(eDriverAll, 0, NULL, &g_fmt);
#else
    char filename[1024];
    sprintf(filename, "%s/SimpleTest.log", outputDirectory);
    g_pLog = new CEGenLogger(eDriverAll, 0, filename, &g_fmt);
#endif

    pthread_exit(NULL); //exit with waiting detached threads.
}

void tpce_worker::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
					   TBrokerVolumeFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CBrokerVolumeDB::DoBrokerVolumeFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char   broker_name[cB_NAME_len+1];
    double volume;

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L1];
    rc = SQLExecute(stmt);
#else
    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

    BeginTxn();


    /* SELECT b_name, SUM(tr_qty * tr_bid_price)
       FROM trade_request, sector, industry, company, broker, security
       WHERE tr_b_id = b_id
         AND tr_s_symb = s_symb
         AND s_co_id = co_id
         AND co_in_id = in_id
         AND sc_id = in_sc_id
         AND b_name IN (%s..)
         AND sc_name = '%s'
       GROUP BY b_name
       ORDER BY 2 DESC */

    stmt = m_Stmt;
    ostringstream osBVF1_1;
#ifdef ORACLE_ODBC
    osBVF1_1 << "SELECT /*+ USE_NL(trade_request sector industry company broker security) */ b_name, SUM(tr_qty * tr_bid_price) price_sum " <<
	"FROM trade_request, sector, industry, company, broker, security " <<
	"WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id AND co_in_id = in_id " <<
	"AND sc_id = in_sc_id AND b_name IN ('";
#else
    osBVF1_1 << "SELECT b_name, SUM(tr_qty * tr_bid_price) AS price_sum " <<
	"FROM trade_request, sector, industry, company, broker, security " <<
	"WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id AND co_in_id = in_id " <<
	"AND sc_id = in_sc_id AND b_name IN ('";
#endif
    for(int i = 0; i < max_broker_list_len; ++i)
    {
	if(pIn->broker_list[i][0] == 0)
	    break;

	if(i)
	    osBVF1_1 << "', '";

	osBVF1_1 << pIn->broker_list[i];
    }
    osBVF1_1 << "') AND sc_name = '" << pIn->sector_name <<
	"' GROUP BY b_name ORDER BY price_sum DESC";
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osBVF1_1.str().c_str()), SQL_NTS);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, broker_name, cB_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &volume, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_broker_list_len)
    {
	strncpy(pOut->broker_name[i], broker_name, cB_NAME_len+1);
	pOut->volume[i] = volume;
	i++;

	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    CommitTxn();

    pOut->list_len = i;
    pOut->status = CBaseTxnErr::SUCCESS;
}

