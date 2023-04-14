// MEESUT.h
//   2008 Yasufumi Kinoshita

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"

namespace TPCE
{

template <typename T>
class InputBuffer {
  std::queue<T*> buffer;
  int size, first, last;

 public:
  InputBuffer() : size(0), first(0), last(0){};
  bool isEmpty() {
    {
      //	    CRITICAL_SECTION(meesut_cs, buffer_lock);
      //// XXX. Assuming CMEE is thread-local, CS is not necessary.
      return buffer.empty();
    }
  }
  T* get() {
    {
      //	    CRITICAL_SECTION(meesut_cs, buffer_lock);
      if (buffer.empty()) return NULL;
      T* tmp = buffer.front();
      buffer.pop();
      return tmp;
    }
  }

  void put(T* tmp) {
    {
      //	    CRITICAL_SECTION(meesut_cs, buffer_lock);
      buffer.push(tmp);
    }
  }
};

class MFBuffer : public InputBuffer<TMarketFeedTxnInput> {};

class TRBuffer : public InputBuffer<TTradeResultTxnInput> {};

class CMEESUT : public CMEESUTInterface
{
 private:
    char m_szHost[iMaxHostname];
    char m_szDBName[iMaxDBName];
    char m_szDBUser[iMaxDBName];
    char m_szDBPass[iMaxDBName];

 public:
    unsigned int m_CountTradeResult[4];
    unsigned int m_CountMarketFeed[4];

    CMEESUT(const char *szHost, const char *szDBName,
	    const char *szDBUser, const char *szDBPass,
	    INT32 InitialThreads, INT32 MaxThreads);
    ~CMEESUT();

    virtual bool TradeResult( PTradeResultTxnInput pTxnInput );
    virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );
};

class myMEESUT : public CMEESUTInterface
{
  MFBuffer* MFQueue;
  TRBuffer* TRQueue;

 public:
  void setMFQueue(MFBuffer* p) { MFQueue = p; }
  void setTRQueue(TRBuffer* p) { TRQueue = p; }

  bool TradeResult(TPCE::PTradeResultTxnInput pTxnInput) {
    TPCE::PTradeResultTxnInput trInput = new TPCE::TTradeResultTxnInput();
    memcpy(trInput, pTxnInput, sizeof(TPCE::TTradeResultTxnInput));
    TRQueue->put(trInput);
    return true;
  }

  bool MarketFeed(TPCE::PMarketFeedTxnInput pTxnInput) {
    TPCE::PMarketFeedTxnInput mfInput = new TPCE::TMarketFeedTxnInput();
    memcpy(mfInput, pTxnInput, sizeof(TPCE::TMarketFeedTxnInput));
    MFQueue->put(mfInput);
    return true;
  }
};

}   // namespace TPCE

#endif //MEE_SUT_H

