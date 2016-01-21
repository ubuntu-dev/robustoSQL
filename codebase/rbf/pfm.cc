#include "pfm.h"
#include <cstdio>
#include <iostream>
#include <cmath>
using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;

}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	int size = fileName.size();	//converting string to char array
	char str[size+1];
	for(int i=0;i<size;i++){
		str[i]=fileName[i];
	}
	str[size]='\0';		//str is the char array representation of the string fileName
	FILE *fp;
	fp=fopen(str,"rb");
	if(fp!=NULL){
		fclose(fp);
		return -1;
	}
	else{
		fp=fopen(str,"wb");
		if(fp!=NULL){
			fclose(fp);
			return 0;
		}
		fclose(fp);
		return -1;
	}
    return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	int size=fileName.size();
	char str[size+1];
	for(int i=0;i<size;i++){
		str[i]=fileName[i];
	}
	str[size]='\0';
	int delstatus=remove(str);
	if(delstatus!=0) return -1;
	else return 0;
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(fileHandle.fp!=NULL){	//if this filehandle is already handling another file.
		return -1;				//its an error by definition
	}

	//converting string to char array
	int size=fileName.size();
	char str[size+1];
	for(int i=0;i<size;i++){
		str[i]=fileName[i];
	}
	str[size]='\0';

	fileHandle.fp = fopen(str,"r+b");
	if(fileHandle.fp == NULL){
		return -1;
	}
	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fflush(fileHandle.fp);
	int ret=fclose(fileHandle.fp);
	if(ret==0){
		return 0;
	}
	else{
		return -1;
	}
}


FileHandle::FileHandle()
{
	//added by me
	fp=NULL;
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	int pagecount=this->getNumberOfPages();
	if(pageNum >= (unsigned)pagecount){
		return -1;
	}
	int seekstatus=fseek(this->fp,pageNum*4096,SEEK_SET);
	if(seekstatus!=0){
		return -1;
	}
	int readcount=fread(data,sizeof(char),PAGE_SIZE,this->fp);
	if(readcount!=PAGE_SIZE){
		return -1;
	}
	readPageCounter+=1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	int pagecount=this->getNumberOfPages();
	if(pageNum>(unsigned)pagecount){
		return -1;
	}
	int seekstatus=fseek(this->fp,pageNum*PAGE_SIZE,SEEK_SET);
	if(seekstatus!=0){
		return -1;
	}
	int writecount=fwrite(data,sizeof(char),PAGE_SIZE,this->fp);
	if(writecount!=PAGE_SIZE){
		return -1;
	}
	writePageCounter+=1;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	fseek(fp,0,SEEK_END);
	int written=fwrite(data,sizeof(char),PAGE_SIZE,fp);

	if(written!=PAGE_SIZE){
		return -1;
	}
	appendPageCounter+=1;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	fseek(fp,0,SEEK_END);
	long int sizeinbytes=ftell(fp);
    return ceil((unsigned)sizeinbytes/4096);
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount=readPageCounter;
	writePageCount=writePageCounter;
	appendPageCount=appendPageCounter;
	return 0;
}
