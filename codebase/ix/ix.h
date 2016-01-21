#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define ROOT_PAGE_NUM 0
# define LEAF_DIR_SIZE 13 //1 bool, 2 shorts, 2 ints
# define NON_LEAF_DIR_SIZE 5 //1 bool 2 shorts
# define RECORD_ID_SIZE 8 //2 ints
# define PAGE_NUM_SIZE 4 //1 int
# define LEAF_INT_ENTRY_SIZE 12 //3 ints
# define NON_LEAF_INT_ENTRY_SIZE 8 // 2 ints
# define INIT_LEAF_PAGE 0
class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
		PagedFileManager *pfm;
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        RC insert(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute,
        		const void *key, const RID &rid, int keyLength, void *childKey, int &childPageId);
        RC createRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void * key, int keyLength);
        RC searchKeyInNonLeaf(const Attribute &attribute, const void *nodeData, const void *key, int &keyPointer);
        RC createBlankLeafPage(void* data, int nextLeafPage, int prevLeafPage);
        RC checkIfNodeHasSpace(bool &hasSpace, void* nodeData, int keyLength, bool isLeaf);
        RC splitLeafNode(IXFileHandle &ixfileHandle, void *nodeData, int nodePageNum, const Attribute &attribute, const void *key,
        		const RID &rid, int keyLength, void* newChildKey, int &newChildPageId);
        RC insertEntryIntoLeaf(IXFileHandle &ixfileHandle, void* nodeData, int nodePageNum, const Attribute attribute, const void* key, const RID &rid, int keyLength);


        RC splitNonLeafNode(IXFileHandle &ixfileHandle, void * nodeData, int nodePageNum, const Attribute &attribute,
        		void* newChildKey, int &newChildPageId);
        short getOffsetEndForMiddleEntry(short numEntries, const Attribute &attribute, void *nodeData, bool isLeaf);
        void checkForDuplicateKeys(short numEntries, const Attribute &attribute, void *nodeData, short &splitOffset, short &numEntriesToKeep);
        RC findPageToInsert(bool &insertInNewPage, void* pageData, const Attribute &attribute, const void * key, bool isLeaf);
        RC populateNewChildKey(void* key, void*pageData, const Attribute &attribute, bool isLeaf);
        RC deleteFirstEntryFromNonLeafNode(IXFileHandle &ixfileHandle, void *pageData, int pageNum, const Attribute &attribute);
        RC createNewRootAndSwap(IXFileHandle &ixfileHandle, void* oldRootData, int oldRootPageNum,
        		const Attribute &attribute, void *newChildKey, int &newChildPageId);

        RC insertEntryIntoNonLeaf(IXFileHandle &ixfileHandle, void * nodeData, int nodePageNum, const Attribute &attribute,
        		void* newChildKey, int &newChildPageId);

        static RC printBTreeUtil(IXFileHandle &ixfileHandle, int pageNum, const Attribute &attribute, int numTabs);
        static RC printLeafNode(void* nodeData, const Attribute &attribute, int numTabs);
        static RC printNonLeafNode(void* nodeData, const Attribute &attribute, int numTabs);

        RC deleteEntry(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute,
        		const void *key, const RID &rid);
        RC deleteEntryFromLeaf(IXFileHandle &ixfileHandle, void *nodeData, int nodePageNum,
        		const Attribute &attibute, const void *key, const RID &rid);
        void findLeafPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, int &leafPageNum);
        void updateOffset(const void* lowKey, bool lowKeyInclusive, IX_ScanIterator &ix_ScanIterator);
        int getLeafPageNum(IXFileHandle &ixfileHandle, int nodePageNum, const Attribute &attribute, const void*key);
        RC populateFirstRootData(void* rootData, int leftLeaf, int rightLeaf, const Attribute &attribute, const void* key);
        bool areAllKeysSame(void* nodeData, const Attribute &attribute);
        bool keyRIDPairPresent(void* nodeData, const Attribute &attribute, const void* key, const RID &rid);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};






class IXFileHandle {
    public:
	//added by me
	FileHandle fileHandle;
    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
	RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
	RC appendPage(const void *data);                                    // Append a specific page
	unsigned getNumberOfPages();                                        // Get the number of pages in the file

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {
    public:
		IXFileHandle ixfileHandle;
		Attribute attribute;
		const void *highKey;
		bool highKeyInclusive;
		int currentPageNum;
		int offset;
		short freeSpacePointer;
		bool isEOF;
		void* pageData;
		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        int getNextLeafPage(int pageNum);

        // Terminate index scan
        RC close();
};

#endif
