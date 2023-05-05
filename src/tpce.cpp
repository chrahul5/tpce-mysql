// This file contains the tpce_worker class
//   2008 Yasufumi Kinoshita

#include "../inc/EGenSimpleTest.h"
#include <vector>

#include <stdint.h>

using namespace TPCE;

void tpce_worker::run_test() {
    // for(int i = 0; i < 3; i++) {
    //     this -> broker_volume();
    // }
    // cout << "broker volume transactions done" << endl;

    // for(int i = 0; i < 3; i++) {
    //     try {
    //         this -> customer_position();
    //         cout << "customer position succeded" << endl;
    //     } catch(...) {
    //         cout << "customer position failed" << endl;
    //     }
    // }
    // cout << "customer position transactions done" << endl;

    // for(int i = 0; i < 10; i++) {
    //     try {
    //         this -> market_watch();
    //         cout << "market watch succeded" << endl;
    //     } catch(...) {
    //         cout << "market watch failed" << endl;
    //     }
    // }
    // cout << "market watch transactions done" << endl;

    for(int i = 0; i < 10; i++) {
        try {
            this -> security_detail();
            cout << "security detail succeded" << endl;
        } catch(...) {
            cout << "security detail failed" << endl;
        }
    }
    cout << "security detail transactions done" << endl;

    // for(int i = 0; i < 10; i++) {
    //     try {
    //         this -> trade_lookup();
    //         cout << "Trade lookup succeded" << endl;
    //     } catch(...) {
    //         cout << "Trade lookup failed" << endl;
    //     }
    // }
    // cout << "Trade lookup transactions done" << endl;

    // for(int i = 0; i < 10; i++) {
    //     try {
    //         this -> trade_update();
    //         cout << "Trade update succeded" << endl;
    //     } catch(...) {
    //         cout << "Trade update failed" << endl;
    //     }
    // }
    // cout << "Trade update transactions done" << endl;

    // for(int i = 0; i < 1; i++) {
    //     try {
    //         this -> trade_result();
    //         cout << "Trade result succeded" << endl;
    //     } catch(...) {
    //         cout << "Trade result failed" << endl;
    //     }
    // }
    // cout << "Trade result transactions done" << endl;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// BrokerVolume implementation.
//////////////////////////////////////////////////////////////////////////////////////////////////////

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
    // cout << "got broker " << broker_name << endl;
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

//////////////////////////////////////////////////////////////////////////////////////////////////////
// CustomerPosition transaction implementation.
//////////////////////////////////////////////////////////////////////////////////////////////////////

void tpce_worker::DoCustomerPositionFrame1(
    const TCustomerPositionFrame1Input *pIn,
    TCustomerPositionFrame1Output *pOut)
{
#ifdef DEBUG
    cout<<"CCustomerPositionDB::DoCustomerPositionFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    TIdent cust_id;

    INT32  c_tier;
    char   date_buf[15];

    TIdent acct_id;

    double cash_bal;
    double asset_total;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
#endif

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

    if(pIn->cust_id == 0)
    {
	/* SELECT c_id
	   FROM   customer
	   WHERE  c_tax_id = '%s' */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_CPF1_1];
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->tax_id, cTAX_ID_len+1), 0,
			  (void*)pIn->tax_id, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osCPF1_1;
#ifdef ORACLE_ODBC
	osCPF1_1 << "SELECT TO_CHAR(c_id) FROM customer WHERE  c_tax_id = '" << pIn->tax_id <<
	    "'";
#else
	osCPF1_1 << "SELECT c_id FROM customer WHERE  c_tax_id = '" << pIn->tax_id <<
	    "'";
#endif
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
            ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	rc = SQLBindCol(stmt, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
	rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &cust_id, 0, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);
#ifdef ORACLE_ODBC
	cust_id = atoll(num_str);
#endif
    }
    else
    {
	cust_id = pIn->cust_id;
    }


    /* SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr,
              c_tier, DATE_FORMAT(c_dob,'%Y-%m-%d'), c_ad_id, c_ctry_1, c_area_1,
              c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2,
              c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3,
              c_email_1, c_email_2
       FROM   customer
       WHERE  c_id = %ld */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_CPF1_2];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", cust_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &cust_id, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osCPF1_2;
#ifdef MYSQL_ODBC
    osCPF1_2 << "SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, DATE_FORMAT(c_dob,'%Y-%m-%d'), c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = " << cust_id;
#elif PGSQL_ODBC
    osCPF1_2 << "SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, TO_CHAR(c_dob,'YYYY-MM-DD'), c_ad_id, c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = " << cust_id;
#elif ORACLE_ODBC
    osCPF1_2 << "SELECT c_st_id, c_l_name, c_f_name, c_m_name, c_gndr, c_tier, TO_CHAR(c_dob,'YYYY-MM-DD'), TO_CHAR(c_ad_id), c_ctry_1, c_area_1, c_local_1, c_ext_1, c_ctry_2, c_area_2, c_local_2, c_ext_2, c_ctry_3, c_area_3, c_local_3, c_ext_3, c_email_1, c_email_2 FROM customer WHERE c_id = " << cust_id;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->c_st_id, cST_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, pOut->c_l_name, cL_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->c_f_name, cF_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, pOut->c_m_name, cM_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, pOut->c_gndr, cGNDR_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_LONG, &c_tier, 0, NULL); //pOut->c_tier
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, date_buf, 15, NULL); //pOut->c_dob
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 8, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 8, SQL_C_SBIGINT, &(pOut->c_ad_id), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, pOut->c_ctry_1, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 10, SQL_C_CHAR, pOut->c_area_1, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 11, SQL_C_CHAR, pOut->c_local_1, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 12, SQL_C_CHAR, pOut->c_ext_1, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 13, SQL_C_CHAR, pOut->c_ctry_2, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 14, SQL_C_CHAR, pOut->c_area_2, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 15, SQL_C_CHAR, pOut->c_local_2, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 16, SQL_C_CHAR, pOut->c_ext_2, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 17, SQL_C_CHAR, pOut->c_ctry_3, cCTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 18, SQL_C_CHAR, pOut->c_area_3, cAREA_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 19, SQL_C_CHAR, pOut->c_local_3, cLOCAL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 20, SQL_C_CHAR, pOut->c_ext_3, cEXT_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 21, SQL_C_CHAR, pOut->c_email_1, cEMAIL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 22, SQL_C_CHAR, pOut->c_email_2, cEMAIL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

#ifdef ORACLE_ODBC
    pOut->c_ad_id = atoll(num_str);
#endif

    pOut->c_tier = (char)c_tier;

    //pOut->c_dob = date_buf;
    sscanf(date_buf,"%4hd-%2hu-%2hu",
	   &(pOut->c_dob.year),
	   &(pOut->c_dob.month),
	   &(pOut->c_dob.day));
    pOut->c_dob.hour = pOut->c_dob.minute = pOut->c_dob.second = 0;
    pOut->c_dob.fraction = 0;


    /* SELECT   ca_id,
                ca_bal,
                COALESCE(SUM(hs_qty * lt_price),0)
       FROM     customer_account
                LEFT OUTER JOIN holding_summary
                             ON hs_ca_id = ca_id,
                last_trade
       WHERE    ca_c_id = %ld
                AND lt_s_symb = hs_s_symb
       GROUP BY ca_id,ca_bal
       ORDER BY 3 ASC
       LIMIT max_acct_len */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_CPF1_3];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", cust_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &cust_id, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*) &max_acct_len, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osCPF1_3;
#ifdef ORACLE_ODBC
    osCPF1_3 << "SELECT * FROM (SELECT TO_CHAR(ca_id), ca_bal, COALESCE(SUM(hs_qty * lt_price),0) price_sum FROM customer_account LEFT OUTER JOIN holding_summary ON hs_ca_id = ca_id, last_trade WHERE ca_c_id = " <<
	cust_id << " AND lt_s_symb = hs_s_symb GROUP BY ca_id,ca_bal ORDER BY price_sum ASC ) WHERE ROWNUM <= " <<
	max_acct_len;
#else
    osCPF1_3 << "SELECT ca_id, ca_bal, COALESCE(SUM(hs_qty * lt_price),0) AS price_sum FROM customer_account LEFT OUTER JOIN holding_summary ON hs_ca_id = ca_id, last_trade WHERE ca_c_id = " <<
	cust_id << " AND lt_s_symb = hs_s_symb GROUP BY ca_id,ca_bal ORDER BY price_sum ASC LIMIT " <<
	max_acct_len;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF1_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &acct_id, 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &cash_bal, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_DOUBLE, &asset_total, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_acct_len)
    {
#ifdef ORACLE_ODBC
	acct_id = atoll(num_str);
#endif
	pOut->acct_id[i] = acct_id;
	pOut->cash_bal[i] = cash_bal;
	pOut->asset_total[i] = asset_total;
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->acct_len = i;
    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    cust_id++;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}

void tpce_worker::DoCustomerPositionFrame2(
    const TCustomerPositionFrame2Input *pIn,
    TCustomerPositionFrame2Output *pOut)
{
#ifdef DEBUG
    cout<<"CCustomerPositionDB::DoCustomerPositionFrame2"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    TTrade trade_id;
    char   symbol[cSYMBOL_len+1];
    INT32  qty;
    char   trade_status[cST_NAME_len+1];
    char   datetime_buf[30]; //hist_dts

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
#endif

    /* SELECT   t_id,
                t_s_symb,
                t_qty,
                st_name,
                DATE_FORMAT(th_dts,'%Y-%m-%d %H:%i:%s.%f')
       FROM     (SELECT   t_id AS id
                 FROM     trade
                 WHERE    t_ca_id = %ld
                 ORDER BY t_dts DESC
                 LIMIT 10) AS t,
                trade,
                trade_history,
                status_type
       WHERE    t_id = id
                AND th_t_id = t_id
                AND st_id = th_st_id
       ORDER BY th_dts DESC
       LIMIT max_hist_len */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_CPF2_1];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", pIn->acct_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->acct_id), NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*) &max_hist_len, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osCPF2_1;
#ifdef MYSQL_ODBC
    osCPF2_1 << "SELECT t_id, t_s_symb, t_qty, st_name, DATE_FORMAT(th_dts,'%Y-%m-%d %H:%i:%s.%f') FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " ORDER BY t_dts DESC LIMIT 10) AS t, trade, trade_history, status_type FORCE INDEX(PRIMARY) WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC LIMIT " <<
	max_hist_len;
#elif PGSQL_ODBC
    osCPF2_1 << "SELECT t_id, t_s_symb, t_qty, st_name, TO_CHAR(th_dts,'YYYY-MM-DD HH24:MI:SS.US') FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " ORDER BY t_dts DESC LIMIT 10) AS t, trade, trade_history, status_type WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC LIMIT " <<
	max_hist_len;
#elif ORACLE_ODBC
    osCPF2_1 << "SELECT * FROM (SELECT TO_CHAR(t_id), t_s_symb, t_qty, st_name, TO_CHAR(th_dts,'YYYY-MM-DD HH24:MI:SS.FF6') FROM (SELECT * FROM (SELECT t_id AS id FROM trade WHERE t_ca_id = " <<
	pIn->acct_id << " ORDER BY t_dts DESC) WHERE ROWNUM <= 10), trade, trade_history, status_type WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id ORDER BY th_dts DESC) WHERE ROWNUM <= " <<
	max_hist_len;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osCPF2_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 1, SQL_C_SBIGINT, &trade_id, 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, symbol, cSYMBOL_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_LONG, &qty, 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, trade_status, cST_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, datetime_buf, 30, NULL); //hist_dts
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    int i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_hist_len)
    {
#ifdef ORACLE_ODBC
	trade_id = atoll(num_str);
#endif
	pOut->trade_id[i] = trade_id;
	strncpy(pOut->symbol[i], symbol, cSYMBOL_len+1);
	pOut->qty[i] = qty;
	strncpy(pOut->trade_status[i], trade_status, cST_NAME_len+1);
	sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
	       &(pOut->hist_dts[i].year),
	       &(pOut->hist_dts[i].month),
	       &(pOut->hist_dts[i].day),
	       &(pOut->hist_dts[i].hour),
	       &(pOut->hist_dts[i].minute),
	       &(pOut->hist_dts[i].second),
	       &(pOut->hist_dts[i].fraction));
	pOut->hist_dts[i].fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    CommitTxn();

    pOut->hist_len = i;
    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}

void tpce_worker::DoCustomerPositionFrame3(
    TCustomerPositionFrame3Output *pOut)
{
#ifdef DEBUG
    cout<<"CCustomerPositionDB::DoCustomerPositionFrame3"<<endl;
#endif
    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// SecurityDetail transaction implementation.
//////////////////////////////////////////////////////////////////////////////////////////////////////

void tpce_worker::DoSecurityDetailFrame1(
    const TSecurityDetailFrame1Input *pIn,
    TSecurityDetailFrame1Output *pOut)
{
    cout << "i am doing this" << endl;
#ifdef DEBUG
    cout<<"CSecurityDetailDB::DoSecurityDetailFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    int i;

    TIdent co_id;
    char   date_buf1[15]; //pOut->open_date, pOut->fin[i].start_date, pOut->day[i].date
    char   date_buf2[15]; //pOut->start_date
    char   date_buf3[15]; //pOut->ex_date
    char   date_buf4[15]; //pOut->s52_wk_high_date
    char   date_buf5[15]; //pOut->s52_wk_low_date
    char   datetime_buf[30]; //pOut->news[i].dts

    INT64  out_basic;
    INT64  out_dilut;

    //max_comp_len
    char   cp_co_name[cCO_NAME_len+1];
    char   cp_in_name[cIN_NAME_len+1];

    //max_fin_len
    TFinInfo fin;

    //max_rows_to_return (<= max_day_len)
    TDailyHistory day;

    //max_news_len
    TNews news;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
#endif
    char num_str[30];
    char num_str2[30];
#endif

    // Isolation level required by Clause 7.4.1.3
#ifdef USE_PREPARE
    cout << "i am using prepare too" << endl;
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_ISO_L1];
    rc = SQLExecute(stmt);
#else
    cout << "i am not using prepare too" << endl;
    stmt = m_Stmt;
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", SQL_NTS);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        cout << "this failed " << rc << endl;
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
        
    SQLCloseCursor(stmt);

    BeginTxn();

    /* SELECT s_name, co_id, co_name, co_sp_rate, co_ceo,
              co_desc, DATE_FORMAT(co_open_date,'%Y-%m-%d'), co_st_id, ca.ad_line1, ca.ad_line2,
              zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, s_num_out,
              DATE_FORMAT(s_start_date,'%Y-%m-%d'), DATE_FORMAT(s_exch_date,'%Y-%m-%d'),
              s_pe, s_52wk_high, DATE_FORMAT(s_52wk_high_date,'%Y-%m-%d'),
              s_52wk_low, DATE_FORMAT(s_52wk_low_date,'%Y-%m-%d'), s_dividend, s_yield, zea.zc_div,
              ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code,
              ex_close, ex_desc, ex_name, ex_num_symb, ex_open
       FROM   security,
              company,
              address ca,
              address ea,
              zip_code zca,
              zip_code zea,
              exchange
       WHERE  s_symb = '%s'
              AND co_id = s_co_id
              AND ca.ad_id = co_ad_id
              AND ea.ad_id = ex_ad_id
              AND ex_id = s_ex_id
              AND ca.ad_zc_code = zca.zc_code
              AND ea.ad_zc_code = zea.zc_code */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_1];
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->symbol, cSYMBOL_len+1), 0,
		      (void*)(pIn->symbol), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osSDF1_1;
#ifdef MYSQL_ODBC
    osSDF1_1 << "SELECT s_name, co_id, co_name, co_sp_rate, co_ceo, co_desc, DATE_FORMAT(co_open_date,'%Y-%m-%d'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, s_num_out, DATE_FORMAT(s_start_date,'%Y-%m-%d'), DATE_FORMAT(s_exch_date,'%Y-%m-%d'), s_pe, s_52wk_high, DATE_FORMAT(s_52wk_high_date,'%Y-%m-%d'), s_52wk_low, DATE_FORMAT(s_52wk_low_date,'%Y-%m-%d'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = '" <<
#elif PGSQL_ODBC
    osSDF1_1 << "SELECT s_name, co_id, co_name, co_sp_rate, co_ceo, co_desc, TO_CHAR(co_open_date,'YYYY-MM-DD'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, s_num_out, TO_CHAR(s_start_date,'YYYY-MM-DD'), TO_CHAR(s_exch_date,'YYYY-MM-DD'), s_pe, s_52wk_high, TO_CHAR(s_52wk_high_date,'YYYY-MM-DD'), s_52wk_low, TO_CHAR(s_52wk_low_date,'YYYY-MM-DD'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = '" <<
#elif ORACLE_ODBC
    osSDF1_1 << "SELECT s_name, TO_CHAR(co_id), co_name, co_sp_rate, co_ceo, co_desc, TO_CHAR(co_open_date,'YYYY-MM-DD'), co_st_id, ca.ad_line1, ca.ad_line2, zca.zc_town, zca.zc_div, ca.ad_zc_code, ca.ad_ctry, TO_CHAR(s_num_out), TO_CHAR(s_start_date,'YYYY-MM-DD'), TO_CHAR(s_exch_date,'YYYY-MM-DD'), s_pe, s_52wk_high, TO_CHAR(s_52wk_high_date,'YYYY-MM-DD'), s_52wk_low, TO_CHAR(s_52wk_low_date,'YYYY-MM-DD'), s_dividend, s_yield, zea.zc_div, ea.ad_ctry, ea.ad_line1, ea.ad_line2, zea.zc_town, ea.ad_zc_code, ex_close, ex_desc, ex_name, ex_num_symb, ex_open FROM security, company, address ca, address ea, zip_code zca, zip_code zea, exchange WHERE s_symb = '" <<
#endif
	pIn->symbol << "' AND co_id = s_co_id AND ca.ad_id = co_ad_id AND ea.ad_id = ex_ad_id AND ex_id = s_ex_id AND ca.ad_zc_code = zca.zc_code AND ea.ad_zc_code = zea.zc_code";
    cout << "executing query " << osSDF1_1.str().c_str() << endl;
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, pOut->s_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 2, SQL_C_SBIGINT, &co_id, 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, pOut->co_name, cCO_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_CHAR, pOut->sp_rate, cSP_RATE_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, pOut->ceo_name, cCEO_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_CHAR, pOut->co_desc, cCO_DESC_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_CHAR, date_buf1, 15, NULL); //pOut->open_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 8, SQL_C_CHAR, pOut->co_st_id, cST_ID_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_CHAR, pOut->co_ad_line1, cAD_LINE_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,10, SQL_C_CHAR, pOut->co_ad_line2, cAD_LINE_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,11, SQL_C_CHAR, pOut->co_ad_town, cAD_TOWN_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,12, SQL_C_CHAR, pOut->co_ad_div, cAD_DIV_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,13, SQL_C_CHAR, pOut->co_ad_zip, cAD_ZIP_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,14, SQL_C_CHAR, pOut->co_ad_cty, cAD_CTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt,15, SQL_C_CHAR, num_str2, 30, NULL);
#else
    rc = SQLBindCol(stmt,15, SQL_C_SBIGINT, &(pOut->num_out), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,16, SQL_C_CHAR, date_buf2, 15, NULL); //pOut->start_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,17, SQL_C_CHAR, date_buf3, 15, NULL); //pOut->ex_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,18, SQL_C_DOUBLE, &(pOut->pe_ratio), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,19, SQL_C_DOUBLE, &(pOut->s52_wk_high), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,20, SQL_C_CHAR, date_buf4, 15, NULL); //pOut->s52_wk_high_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,21, SQL_C_DOUBLE, &(pOut->s52_wk_low), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,22, SQL_C_CHAR, date_buf5, 15, NULL); //pOut->s52_wk_low_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,23, SQL_C_DOUBLE, &(pOut->divid), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,24, SQL_C_DOUBLE, &(pOut->yield), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,25, SQL_C_CHAR, pOut->ex_ad_div, cAD_DIV_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,26, SQL_C_CHAR, pOut->ex_ad_cty, cAD_CTRY_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,27, SQL_C_CHAR, pOut->ex_ad_line1, cAD_LINE_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,28, SQL_C_CHAR, pOut->ex_ad_line2, cAD_LINE_len+1, &m_DummyInd);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,29, SQL_C_CHAR, pOut->ex_ad_town, cAD_TOWN_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,30, SQL_C_CHAR, pOut->ex_ad_zip, cAD_ZIP_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,31, SQL_C_LONG, &(pOut->ex_close), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,32, SQL_C_CHAR, pOut->ex_desc, cEX_DESC_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,33, SQL_C_CHAR, pOut->ex_name, cS_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,34, SQL_C_LONG, &(pOut->ex_num_symb), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,35, SQL_C_LONG, &(pOut->ex_open), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */) {
        cout << rc << " return result" << " failed " <<  endl;
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    } else {
        cout << rc << " return success "  <<  endl;
    }
        
    SQLCloseCursor(stmt);

#ifdef ORACLE_ODBC
    co_id = atoll(num_str);
    pOut->num_out = atoll(num_str2);
#endif

    //pOut->open_date = date_buf1
    sscanf(date_buf1,"%4hd-%2hu-%2hu",
           &(pOut->open_date.year),
           &(pOut->open_date.month),
           &(pOut->open_date.day));
    pOut->open_date.hour = pOut->open_date.minute = pOut->open_date.second = 0;
    pOut->open_date.fraction = 0;

    //pOut->start_date = date_buf2
    sscanf(date_buf2,"%4hd-%2hu-%2hu",
           &(pOut->start_date.year),
           &(pOut->start_date.month),
           &(pOut->start_date.day));
    pOut->start_date.hour = pOut->start_date.minute = pOut->start_date.second = 0;
    pOut->start_date.fraction = 0;

    //pOut->ex_date = date_buf3
    sscanf(date_buf3,"%4hd-%2hu-%2hu",
           &(pOut->ex_date.year),
           &(pOut->ex_date.month),
           &(pOut->ex_date.day));
    pOut->ex_date.hour = pOut->ex_date.minute = pOut->ex_date.second = 0;
    pOut->ex_date.fraction = 0;

    //pOut->s52_wk_high_date = date_buf4
    sscanf(date_buf4,"%4hd-%2hu-%2hu",
           &(pOut->s52_wk_high_date.year),
           &(pOut->s52_wk_high_date.month),
           &(pOut->s52_wk_high_date.day));
    pOut->s52_wk_high_date.hour = pOut->s52_wk_high_date.minute = pOut->s52_wk_high_date.second = 0;
    pOut->s52_wk_high_date.fraction = 0;

    //pOut->s52_wk_low_date = date_buf5
    sscanf(date_buf5,"%4hd-%2hu-%2hu",
           &(pOut->s52_wk_low_date.year),
           &(pOut->s52_wk_low_date.month),
           &(pOut->s52_wk_low_date.day));
    pOut->s52_wk_low_date.hour = pOut->s52_wk_low_date.minute = pOut->s52_wk_low_date.second = 0;
    pOut->s52_wk_low_date.fraction = 0;



    /* SELECT co_name,
              in_name
       FROM   company_competitor,
              company,
              industry
       WHERE  cp_co_id = %s
              AND co_id = cp_comp_co_id
              AND in_id = cp_in_id
       LIMIT max_comp_len */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_2];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", co_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &co_id, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*)&max_comp_len, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osSDF1_2;
#ifdef ORACLE_ODBC
    osSDF1_2 << "SELECT * FROM (SELECT co_name, in_name FROM company_competitor, company, industry WHERE cp_co_id = " <<
	co_id << " AND co_id = cp_comp_co_id AND in_id = cp_in_id) WHERE ROWNUM <= " <<
	max_comp_len;
#else
    osSDF1_2 << "SELECT co_name, in_name FROM company_competitor, company, industry WHERE cp_co_id = " <<
	co_id << " AND co_id = cp_comp_co_id AND in_id = cp_in_id LIMIT " <<
	max_comp_len;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_2.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, cp_co_name, cCO_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_CHAR, cp_in_name, cIN_NAME_len+1, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_comp_len)
    {
	strncpy(pOut->cp_co_name[i], cp_co_name, cCO_NAME_len+1);
	strncpy(pOut->cp_in_name[i], cp_in_name, cIN_NAME_len+1);
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);
    //MEMO: "i" equals always 3 here?



    /* SELECT   fi_year, fi_qtr, DATE_FORMAT(fi_qtr_start_date,'%Y-%m-%d'), fi_revenue, fi_net_earn,
                fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets,
                fi_liability, fi_out_basic, fi_out_dilut
       FROM     financial
       WHERE    fi_co_id = %s
       ORDER BY fi_year ASC,
                fi_qtr
       LIMIT max_fin_len */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_3];
#ifdef ORACLE_ODBC
    snprintf(param_num_str, 30, "%lld", co_id);
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
    rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &co_id, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*)&max_fin_len, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osSDF1_3;
#ifdef MYSQL_ODBC
    osSDF1_3 << "SELECT fi_year, fi_qtr, DATE_FORMAT(fi_qtr_start_date,'%Y-%m-%d'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, fi_out_basic, fi_out_dilut FROM financial WHERE fi_co_id = " <<
	co_id << " ORDER BY fi_year ASC, fi_qtr LIMIT " <<
	max_fin_len;
#elif PGSQL_ODBC
    osSDF1_3 << "SELECT fi_year, fi_qtr, TO_CHAR(fi_qtr_start_date,'YYYY-MM-DD'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, fi_out_basic, fi_out_dilut FROM financial WHERE fi_co_id = " <<
	co_id << " ORDER BY fi_year ASC, fi_qtr LIMIT " <<
	max_fin_len;
#elif ORACLE_ODBC
    osSDF1_3 << "SELECT * FROM (SELECT fi_year, fi_qtr, TO_CHAR(fi_qtr_start_date,'YYYY-MM-DD'), fi_revenue, fi_net_earn, fi_basic_eps, fi_dilut_eps, fi_margin, fi_inventory, fi_assets, fi_liability, TO_CHAR(fi_out_basic), TO_CHAR(fi_out_dilut) FROM financial WHERE fi_co_id = " <<
	co_id << " ORDER BY fi_year ASC, fi_qtr) WHERE ROWNUM <= " <<
	max_fin_len;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_3.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_LONG, &(fin.year), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_LONG, &(fin.qtr), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, date_buf1, 15, NULL); //pOut->fin[i].start_date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_DOUBLE, &(fin.rev), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 5, SQL_C_DOUBLE, &(fin.net_earn), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 6, SQL_C_DOUBLE, &(fin.basic_eps), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 7, SQL_C_DOUBLE, &(fin.dilut_eps), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 8, SQL_C_DOUBLE, &(fin.margin), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 9, SQL_C_DOUBLE, &(fin.invent), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,10, SQL_C_DOUBLE, &(fin.assets), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt,11, SQL_C_DOUBLE, &(fin.liab), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt,12, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt,12, SQL_C_SBIGINT, &out_basic, 0, NULL); //MEMO: fi_out_basic:S_COUNT_T but fin.out_basic:double....
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt,13, SQL_C_CHAR, num_str2, 30, NULL);
#else
    rc = SQLBindCol(stmt,13, SQL_C_SBIGINT, &out_dilut, 0, NULL); //MEMO: fi_out_dilut:S_COUNT_T but fin.out_dilut:double....
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < max_fin_len)
    {
#ifdef ORACLE_ODBC
	out_basic = atoll(num_str);
	out_dilut = atoll(num_str2);
#endif

	pOut->fin[i].year = fin.year;
	pOut->fin[i].qtr = fin.qtr;

	//pOut->fin[i].start_date = date_buf1
	sscanf(date_buf1,"%4hd-%2hu-%2hu",
	       &(pOut->fin[i].start_date.year),
	       &(pOut->fin[i].start_date.month),
	       &(pOut->fin[i].start_date.day));
	pOut->fin[i].start_date.hour = pOut->fin[i].start_date.minute = pOut->fin[i].start_date.second = 0;
	pOut->fin[i].start_date.fraction = 0;

	pOut->fin[i].rev = fin.rev;
	pOut->fin[i].net_earn = fin.net_earn;
	pOut->fin[i].basic_eps = fin.basic_eps;
	pOut->fin[i].dilut_eps = fin.dilut_eps;
	pOut->fin[i].margin = fin.margin;
	pOut->fin[i].invent = fin.invent;
	pOut->fin[i].assets = fin.assets;
	pOut->fin[i].liab = fin.liab;
	//pOut->fin[i].out_basic = fin.out_basic;
	pOut->fin[i].out_basic = (double)out_basic;
	//pOut->fin[i].out_dilut = fin.out_dilut;
	pOut->fin[i].out_dilut = (double)out_dilut;
	i++;

	rc = SQLFetch(stmt);
        if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
            ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->fin_len = i;


    /* SELECT   DATE_FORMAT(dm_date,'%Y-%m-%d'),
                dm_close,
                dm_high,
                dm_low,
                dm_vol
       FROM     daily_market
       WHERE    dm_s_symb = '%s'
                AND dm_date >= '%s'
       ORDER BY dm_date ASC
       LIMIT max_rows_to_return */

    char start_day[15];
    snprintf(start_day, 15, "%04hd-%02hu-%02hu",
	     pIn->start_day.year,
	     pIn->start_day.month,
	     pIn->start_day.day);

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_4];
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->symbol, cSYMBOL_len+1), 0,
		      (void*)(pIn->symbol), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_day, 15), 0, start_day, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindParam(stmt, 3, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*)&(pIn->max_rows_to_return), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osSDF1_4;
#ifdef MYSQL_ODBC
    osSDF1_4 << "SELECT DATE_FORMAT(dm_date,'%Y-%m-%d'), dm_close, dm_high, dm_low, dm_vol FROM daily_market WHERE dm_s_symb = '" <<
	pIn->symbol << "' AND dm_date >= '" <<
	start_day << "' ORDER BY dm_date ASC LIMIT " <<
	pIn->max_rows_to_return;
#elif PGSQL_ODBC
    osSDF1_4 << "SELECT TO_CHAR(dm_date,'YYYY-MM-DD'), dm_close, dm_high, dm_low, dm_vol FROM daily_market WHERE dm_s_symb = '" <<
	pIn->symbol << "' AND dm_date >= '" <<
	start_day << "' ORDER BY dm_date ASC LIMIT " <<
	pIn->max_rows_to_return;
#elif ORACLE_ODBC
    osSDF1_4 << "SELECT * FROM (SELECT TO_CHAR(dm_date,'YYYY-MM-DD'), dm_close, dm_high, dm_low, TO_CHAR(dm_vol) FROM daily_market WHERE dm_s_symb = '" <<
	pIn->symbol << "' AND dm_date >= '" <<
	start_day << "' ORDER BY dm_date ASC) WHERE ROWNUM <= " <<
	pIn->max_rows_to_return;
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_4.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_CHAR, date_buf1, 15, NULL); //pOut->day[i].date
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &(day.close), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 3, SQL_C_DOUBLE, &(day.high), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 4, SQL_C_DOUBLE, &(day.low), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 5, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 5, SQL_C_SBIGINT, &(day.vol), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

    i = 0;
    while(rc != SQL_NO_DATA_FOUND && i < pIn->max_rows_to_return)
    {
#ifdef ORACLE_ODBC
	day.vol = atoll(num_str);
#endif

	//pOut->day[i].date = date_buf1
	sscanf(date_buf1,"%4hd-%2hu-%2hu",
	       &(pOut->day[i].date.year),
	       &(pOut->day[i].date.month),
	       &(pOut->day[i].date.day));
	pOut->day[i].date.hour = pOut->day[i].date.minute = pOut->day[i].date.second = 0;
	pOut->day[i].date.fraction = 0;

	pOut->day[i].close = day.close;
	pOut->day[i].high = day.high;
	pOut->day[i].low = day.low;
	pOut->day[i].vol = day.vol;
	i++;

	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    }
    SQLCloseCursor(stmt);

    pOut->day_len = i;


    /* SELECT lt_price,
              lt_open_price,
              lt_vol
       FROM   last_trade
       WHERE  lt_s_symb = '%s' */

#ifdef USE_PREPARE
    stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_5];
    rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->symbol, cSYMBOL_len+1), 0,
		      (void*)(pIn->symbol), NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLExecute(stmt);
#else // !USE_PREPARE
    stmt = m_Stmt;
    ostringstream osSDF1_5;
#ifdef ORACLE_ODBC
    osSDF1_5 << "SELECT lt_price, lt_open_price, TO_CHAR(lt_vol) FROM last_trade WHERE lt_s_symb = '" <<
	pIn->symbol << "'";
#else
    osSDF1_5 << "SELECT lt_price, lt_open_price, lt_vol FROM last_trade WHERE lt_s_symb = '" <<
	pIn->symbol << "'";
#endif
    rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_5.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &(pOut->last_price), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &(pOut->last_open), 0, NULL);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
    rc = SQLBindCol(stmt, 3, SQL_C_CHAR, num_str, 30, NULL);
#else
    rc = SQLBindCol(stmt, 3, SQL_C_SBIGINT, &(pOut->last_vol), 0, NULL);
#endif
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
        ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    rc = SQLFetch(stmt);
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
        ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
    SQLCloseCursor(stmt);

#ifdef ORACLE_ODBC
    pOut->last_vol = atoll(num_str);
#endif

    if(pIn->access_lob_flag)
    {
	/* SELECT ni_item,
	          DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'),
	          ni_source,
	          ni_author
	   FROM   news_xref,
	          news_item
	   WHERE  ni_id = nx_ni_id
	          AND nx_co_id = %s
	   LIMIT max_news_len */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_6];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", co_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &co_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*)&max_news_len, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osSDF1_6;
#ifdef MYSQL_ODBC
	osSDF1_6 << "SELECT ni_item, DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " LIMIT " <<
	    max_news_len;
#elif PGSQL_ODBC
	osSDF1_6 << "SELECT ni_item, TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " LIMIT " <<
	    max_news_len;
#elif ORACLE_ODBC
	osSDF1_6 << "SELECT * FROM (SELECT ni_item, TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ni_source, ni_author FROM news_xref, news_item WHERE  ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " ) WHERE ROWNUM <= " <<
	    max_news_len;
#endif
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_6.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_CHAR, news.item, cNI_ITEM_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->news[i].dts
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_CHAR, news.src, cNI_SOURCE_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 4, SQL_C_CHAR, news.auth, cNI_AUTHOR_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	i = 0;
	while(rc != SQL_NO_DATA_FOUND && i < max_news_len)
	{
	    strncpy(pOut->news[i].item, news.item, cNI_ITEM_len+1);

	    //pOut->news[i].dts = datetime_buf
	    sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
		   &(pOut->news[i].dts.year),
		   &(pOut->news[i].dts.month),
		   &(pOut->news[i].dts.day),
		   &(pOut->news[i].dts.hour),
		   &(pOut->news[i].dts.minute),
		   &(pOut->news[i].dts.second),
		   &(pOut->news[i].dts.fraction));
	    pOut->news[i].dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.

	    strncpy(pOut->news[i].src, news.src, cNI_SOURCE_len+1);
	    strncpy(pOut->news[i].auth, news.auth, cNI_AUTHOR_len+1);

	    pOut->news[i].headline[0] = 0; //""
	    pOut->news[i].summary[0] = 0; //""
	    i++;

	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	}
	SQLCloseCursor(stmt);

	pOut->news_len = i;
    }
    else
    {
	/* SELECT DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'),
	          ni_source,
	          ni_author,
	          ni_headline,
	          ni_summary
	   FROM   news_xref,
	          news_item
	   WHERE  ni_id = nx_ni_id
	          AND nx_co_id = %s
	   LIMIT max_news_len */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_SDF1_7];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", co_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &co_id, NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_LONG, SQL_INTEGER, 0, 0, (void*)&max_news_len, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	stmt = m_Stmt;
	ostringstream osSDF1_7;
#ifdef MYSQL_ODBC
	osSDF1_7 << "SELECT DATE_FORMAT(ni_dts, '%Y-%m-%d %H:%i:%s.%f'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " LIMIT " <<
	    max_news_len;
#elif PGSQL_ODBC
	osSDF1_7 << "SELECT TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.US'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " LIMIT " <<
	    max_news_len;
#elif ORACLE_ODBC
	osSDF1_7 << "SELECT * FROM (SELECT TO_CHAR(ni_dts, 'YYYY-MM-DD HH24:MI:SS.FF6'), ni_source, ni_author, ni_headline, ni_summary FROM news_xref, news_item WHERE ni_id = nx_ni_id AND nx_co_id = " <<
	    co_id << " ) WHERE ROWNUM <= " <<
	    max_news_len;
#endif
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osSDF1_7.str().c_str()), SQL_NTS);
#endif // USE_PREPARE

	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_CHAR, datetime_buf, 30, NULL); //pOut->news[i].dts
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_CHAR, news.src, cNI_SOURCE_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 3, SQL_C_CHAR, news.auth, cNI_AUTHOR_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 4, SQL_C_CHAR, news.headline, cNI_HEADLINE_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 5, SQL_C_CHAR, news.summary, cNI_SUMMARY_len+1, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);

	i = 0;
	while(rc != SQL_NO_DATA_FOUND && i < max_news_len)
	{
	    pOut->news[i].item[0] = 0; //""

	    //pOut->news[i].dts = datetime_buf
	    sscanf(datetime_buf, "%4hd-%2hu-%2hu %2hu:%2hu:%2hu.%u",
		   &(pOut->news[i].dts.year),
		   &(pOut->news[i].dts.month),
		   &(pOut->news[i].dts.day),
		   &(pOut->news[i].dts.hour),
		   &(pOut->news[i].dts.minute),
		   &(pOut->news[i].dts.second),
		   &(pOut->news[i].dts.fraction));
	    pOut->news[i].dts.fraction *= 1000; //MySQL %f:micro sec.  EGen(ODBC) fraction:nano sec.

	    strncpy(pOut->news[i].src, news.src, cNI_SOURCE_len+1);
	    strncpy(pOut->news[i].auth, news.auth, cNI_AUTHOR_len+1);
	    strncpy(pOut->news[i].headline, news.headline, cNI_HEADLINE_len+1);
	    strncpy(pOut->news[i].summary, news.summary, cNI_SUMMARY_len+1);
	    i++;

	    rc = SQLFetch(stmt);
	    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO && rc != SQL_NO_DATA_FOUND)
		ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	}
	SQLCloseCursor(stmt);

	pOut->news_len = i;
    }

    CommitTxn();

    pOut->status = CBaseTxnErr::SUCCESS;

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    co_id++;
    start_day[14]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
#endif
#endif
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Market Watch transaction implementation.
//////////////////////////////////////////////////////////////////////////////////////////////////////

void tpce_worker::DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
					 TMarketWatchFrame1Output *pOut)
{
cout << "I am being called" << endl;
#ifdef DEBUG
    cout<<"CMarketWatchDB::DoMarketWatchFrame1"<<endl;
#endif
    SQLHSTMT stmt;

    SQLRETURN rc;

    char start_date[15]; //pIn->start_day

    double old_mkt_cap = 0.0;
    double new_mkt_cap = 0.0;

#ifdef ORACLE_ODBC
#ifdef USE_PREPARE
    char param_num_str[30];
    char param_num_str2[30];
#endif
#endif

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

    pOut->status = CBaseTxnErr::SUCCESS;

    // start_date = pIn->start_day
    snprintf(start_date, 15, "%04hd-%02hu-%02hu",
	     pIn->start_day.year,
	     pIn->start_day.month,
	     pIn->start_day.day);

#ifndef USE_PREPARE
    ostringstream osMWF1_1;
#endif
    if(pIn->c_id != 0)
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM watch_item, watch_list,
	        last_trade, security, daily_market
	   WHERE wl_c_id = %lu
	         AND wi_wl_id = wl_id
		 AND dm_s_symb = wi_s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb
		 AND s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1a];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->c_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->c_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM watch_item, watch_list, last_trade, security, daily_market WHERE wl_c_id = " <<
	    pIn->c_id << " AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb";
#endif // USE_PREPARE
    }
    else if(pIn->industry_name[0] != 0) // ""
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM industry, company, security,
	        last_trade, daily_market
	   WHERE in_name = '%s'
	         AND co_in_id = in_id
		 AND co_id BETWEEN %lu AND %lu
		 AND s_co_id = co_id
		 AND dm_s_symb = s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1b];
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_CHAR, strnlen(pIn->industry_name, cIN_NAME_len+1), 0,
			  (void*)(pIn->industry_name), NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->starting_co_id);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 2, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->starting_co_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#ifdef ORACLE_ODBC
	snprintf(param_num_str2, 30, "%lld", pIn->ending_co_id);
	rc = SQLBindParam(stmt, 3, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str2, 30), 0, param_num_str2, NULL);
#else
	rc = SQLBindParam(stmt, 3, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->ending_co_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 4, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM industry, company, security, last_trade, daily_market WHERE in_name = '" <<
	    pIn->industry_name << "' AND co_in_id = in_id AND co_id BETWEEN " <<
	    pIn->starting_co_id << " AND " << pIn->ending_co_id << " AND s_co_id = co_id AND dm_s_symb = s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb";
#endif // USE_PREPARE
    }
    else if(pIn->acct_id != 0)
    {
	/* SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0)
	   FROM holding_summary,
	        last_trade, security, daily_market
	   WHERE hs_ca_id = %lu
	         AND dm_s_symb = hs_s_symb
		 AND dm_date = '%s'
		 AND lt_s_symb = dm_s_symb
		 AND s_symb = dm_s_symb */

#ifdef USE_PREPARE
	stmt = m_pDBConnection->m_pPrepared[CESUT_STMT_MWF1_1c];
#ifdef ORACLE_ODBC
	snprintf(param_num_str, 30, "%lld", pIn->acct_id);
	rc = SQLBindParam(stmt, 1, SQL_C_CHAR, SQL_DECIMAL, strnlen(param_num_str, 30), 0, param_num_str, NULL);
#else
	rc = SQLBindParam(stmt, 1, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, (void*) &(pIn->acct_id), NULL);
#endif
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindParam(stmt, 2, SQL_C_CHAR, SQL_CHAR, strnlen(start_date, 15), 0, start_date, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindParam, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
#else // !USE_PREPARE
	stmt = m_Stmt;
	osMWF1_1 << "SELECT COALESCE( SUM(s_num_out * dm_close), 0), COALESCE( SUM(s_num_out * lt_price), 0) FROM holding_summary, last_trade, security, daily_market WHERE hs_ca_id = " <<
	    pIn->acct_id << " AND dm_s_symb = hs_s_symb AND dm_date = '" <<
	    start_date << "' AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb";
#endif // USE_PREPARE
//    }
//    else
//    {
//	pOut->status = CBaseTxnErr::BAD_INPUT_DATA;
    }

//    if(pOut->status != CBaseTxnErr::BAD_INPUT_DATA)
    {
#ifdef USE_PREPARE
	rc = SQLExecute(stmt);
#else // !USE_PREPARE
	rc = SQLExecDirect(stmt, (SQLCHAR*)(osMWF1_1.str().c_str()), SQL_NTS);
#endif // USE_PREPARE
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eExecDirect, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 1, SQL_C_DOUBLE, &old_mkt_cap, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLBindCol(stmt, 2, SQL_C_DOUBLE, &new_mkt_cap, 0, NULL);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
	    ThrowError(CODBCERR::eBindCol, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	rc = SQLFetch(stmt);
	if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO /* && rc != SQL_NO_DATA_FOUND */)
	    ThrowError(CODBCERR::eFetch, SQL_HANDLE_STMT, stmt, __FILE__, __LINE__);
	SQLCloseCursor(stmt);

	if (old_mkt_cap != 0.0)
	{
	    pOut->pct_change = 100.0 * (new_mkt_cap / old_mkt_cap - 1.0);
	}
	else
	{
	    pOut->pct_change = 0.0;
	}
    }


    CommitTxn();

    //These are dummy to avoid optimize out local values...
#ifdef USE_PREPARE
    start_date[14]=0;
#ifdef ORACLE_ODBC
    param_num_str[29]=0;
    param_num_str2[29]=0;
#endif
#endif
}
