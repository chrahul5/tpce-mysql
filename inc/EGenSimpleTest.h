// EGenSimpleTest.h
//   2008 Yasufumi Kinoshita

#ifndef EGEN_SIMPLE_TEST_H
#define EGEN_SIMPLE_TEST_H

#include <iostream>
#include <fstream>
#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
using namespace std;

#include <vector>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <stdlib.h>

#ifdef ORACLE_ODBC
inline void numeric2bigint(long long* dest, SQL_NUMERIC_STRUCT* src)
{
    *dest = 0;
    for(int i = 0; i < SQL_MAX_NUMERIC_LEN; i++)
    {
	*dest += (unsigned char) src->val[i] * (256 ^ i);
    }
    if(src->scale > 0)
	*dest /= (10 ^ src->scale);
    if(src->scale < 0)
	*dest *= (10 ^ (- src->scale));
    if(!src->sign)
	*dest = - (*dest);
}
#endif

#include <signal.h>
#include <time.h>
#include <sys/times.h>
#define  TIMESTAMP_LEN  80
#define  STRFTIME_FORMAT        "%Y-%m-%d %H:%M:%S"
inline void gettimestamp (char str[], const char *format, size_t len)
{
    time_t t;
    struct tm *datetime;

    t = time(NULL);
    datetime = localtime(&t);

    if ( !strftime(str, len, format, datetime) ) {
	fprintf(stderr, "error writing timestamp to string\n");
	abort();
    }
}

inline void expand_quote(char* dest, const char* src, size_t n)
{
    if (n != 0) {
	register char *d = dest;
	register const char *s = src;

	do {
	    if (*s == '\'') {
		*d++ = '\'';
		if(--n == 0)
		    break;
	    }
	    if ((*d++ = *s++) == 0) {
		*d++ = 0;
		break;
	    }
	} while (--n != 0);
    }
}

enum eGlobalState
{
    INITIAL = 0,
    CLEANUPED,
    MEASURING,
    STOPPING
};

enum eTxnType
{
    TRADE_RESULT = 0,
    MARKET_FEED,

    DATA_MAINTENANCE,

    BROKER_VOLUME,
    CUSTOMER_POSITION,
    MARKET_WATCH,
    SECURITY_DETAIL,
    TRADE_LOOKUP,
    TRADE_ORDER,
    TRADE_STATUS,
    TRADE_UPDATE,

    TRADE_CLEANUP
};

#ifdef ODBC_WRAPPER
#include "odbc_wrapper.h"
#endif

//#include "CThreadErr.h"
//#include "CSocket.h"
//#include "SocketPorts.h"
#include "EGenStandardTypes.h"
#include "MiscConsts.h"
#include "TxnHarnessStructs.h"
//#include "CommonStructs.h"
#include "locking.h"
#include "TxnHarnessSendToMarketInterface.h"
#include "SendToMarket.h"

#include "ODBCERR.h"
#include "DBConnection.h"
#include "TxnDBBase.h"
#include "TradeStatusDB.h"
#include "TradeOrderDB.h"
#include "TradeLookupDB.h"
#include "TradeUpdateDB.h"
#include "CustomerPositionDB.h"
#include "BrokerVolumeDB.h"
#include "SecurityDetailDB.h"
#include "MarketWatchDB.h"

#include "TradeResultDB.h"
#include "MarketFeedDB.h"

#include "DataMaintenanceDB.h"
#include "TradeCleanupDB.h"

#include "EGenNullLogger.h"

#include "DM.h"
#include "DMSUT.h"

#include "MEE.h"
#include "MEESUT.h"

#include "CE.h"
#include "CESUT.h"

#include "TxnHarnessDataMaintenance.h"
#include "TxnHarnessCustomerPosition.h"
#include "TxnHarnessMarketFeed.h"
#include "TxnHarnessMarketWatch.h"
#include "TxnHarnessTradeLookup.h"
#include "TxnHarnessTradeOrder.h"
#include "TxnHarnessTradeResult.h"
#include "TxnHarnessTradeStatus.h"
#include "TxnHarnessTradeUpdate.h"
#include "TxnHarnessBrokerVolume.h"
#include "TxnHarnessSecurityDetail.h"
#include "TxnHarnessTradeCleanup.h"

#include "RtHistogram.h"

//#include "BrokerageHouse.h"
//#include "DriverMarket.h"
//#include "Driver.h"

//#define DEBUG
namespace TPCE {

class tpce_worker : public CTxnDBBase,
                    public CBrokerVolumeDBInterface,
                    public CCustomerPositionDBInterface,
                    // public CMarketFeedDBInterface,
                    // public CMarketWatchDBInterface,
                    public CSecurityDetailDBInterface
                    // public CTradeLookupDBInterface,
                    // public CTradeOrderDBInterface,
                    // public CTradeResultDBInterface,
                    // public CTradeStatusDBInterface,
                    // public CTradeUpdateDBInterface,
                    // public CDataMaintenanceDBInterface,
                    // public CTradeCleanupDBInterface,
                    // public CSendToMarketInterface 
                    {
public:
tpce_worker(CDBConnection *pDBConn, unsigned int worker_id,
    CMEE * mee, MFBuffer * MarketFeedInputBuffer,TRBuffer * TradeResultInputBuffer);

// BrokerVolume transaction implementation.
void broker_volume();

void DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
					   TBrokerVolumeFrame1Output *pOut);

// CustomerPosition transaction implementation.
void customer_position();

void DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *pIn, TCustomerPositionFrame1Output *pOut);

void DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *pIn, TCustomerPositionFrame2Output *pOut);

void DoCustomerPositionFrame3(TCustomerPositionFrame3Output *pOut);

// Security Detail transaction implementation.
void security_detail();

void DoSecurityDetailFrame1(const TSecurityDetailFrame1Input *pIn,
                              TSecurityDetailFrame1Output *pOut);

void run_test();

private:
  uint worker_id;
  CMEE *mee;  // thread-local MEE
  MFBuffer *MarketFeedInputBuffer;
  TRBuffer *TradeResultInputBuffer;
};
}

#endif  // EGEN_SIMPLE_TEST_H
