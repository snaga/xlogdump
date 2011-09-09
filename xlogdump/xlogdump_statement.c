/*
 * xlogdump_statement.c
 *
 * a collection of functions to build/re-produce (fake) SQL statements
 * from xlog records.
 */
#include "xlogdump_statement.h"

#include "access/tupmacs.h"
#include "storage/bufpage.h"

#include "xlogdump_oid2name.h"

static int printField(char *, int, int, uint32);

#if PG_VERSION_NUM < 80300
#define MaxHeapTupleSize  (BLCKSZ - MAXALIGN(sizeof(PageHeaderData)))
#endif

/*
 * Print a insert command that contains all the data on a xl_heap_insert
 */
void
printInsert(xl_heap_insert *xlrecord, uint32 datalen, const char *relName)
{
	char data[MaxHeapTupleSize];
	xl_heap_header hhead;
	int offset;
	bits8 nullBitMap[MaxNullBitmapLen];

	MemSet((char *) data, 0, MaxHeapTupleSize * sizeof(char));
	MemSet(nullBitMap, 0, MaxNullBitmapLen);
	
	if(datalen > MaxHeapTupleSize)
		return;

	/* Copy the heap header into hhead, 
	   the the heap data into data 
	   and the tuple null bitmap into nullBitMap */
	memcpy(&hhead, (char *) xlrecord + SizeOfHeapInsert, SizeOfHeapHeader);
	memcpy(&data, (char *) xlrecord + hhead.t_hoff - 4, datalen);
#if PG_VERSION_NUM >= 80300
	memcpy(&nullBitMap, (bits8 *) xlrecord + SizeOfHeapInsert + SizeOfHeapHeader, BITMAPLEN(HeapTupleHeaderGetNatts(&hhead)) * sizeof(bits8));
#else
#warning "Copying null bitmap is not implemented for 8.2.x." /* FIXME: need to fix for 8.2 */
#endif
	
	printf("INSERT INTO \"%s\" (", relName);
	
	// Get relation field names and types
	if (oid2name_enabled())
	{
		int	i, rows = 0, fieldSize = 0;
		
		rows = relid2attr_begin(relName);

		for(i = 0; i < rows; i++)
		{
			char attname[NAMEDATALEN];
			Oid atttypid;

			relid2attr_fetch(i, attname, &atttypid);

			printf("%s%s", (i == 0 ? "" : ", "), attname);
		}

		printf(") VALUES (");
		offset = 0;

		for(i = 0; i < rows; i++)
		{
			char attname[NAMEDATALEN];
			Oid atttypid;

			relid2attr_fetch(i, attname, &atttypid);

			/* is the attribute value null? */
			if((hhead.t_infomask & HEAP_HASNULL) && (att_isnull(i, nullBitMap)))
			{
				printf("%sNULL", (i == 0 ? "" : ", "));
			}
			else
			{
				printf("%s'", (i == 0 ? "" : ", "));
				if(!(fieldSize = printField(data, offset, atttypid, datalen)))
				{
					printf("'");
					break;
				}
				else
					printf("'");

				offset += fieldSize;
			}
		}
		printf(");\n");

		relid2attr_end();
	}
}

/*
 * Print a update command that contains all the data on a xl_heap_update
 */
void
printUpdate(xl_heap_update *xlrecord, uint32 datalen, const char *relName)
{
	char data[MaxHeapTupleSize];
	xl_heap_header hhead;
	int offset;
	bits8 nullBitMap[MaxNullBitmapLen];

	MemSet((char *) data, 0, MaxHeapTupleSize * sizeof(char));
	MemSet(nullBitMap, 0, MaxNullBitmapLen);
	
	if(datalen > MaxHeapTupleSize)
		return;

	/* Copy the heap header into hhead, 
	   the the heap data into data 
	   and the tuple null bitmap into nullBitMap */
	memcpy(&hhead, (char *) xlrecord + SizeOfHeapUpdate, SizeOfHeapHeader);
	memcpy(&data, (char *) xlrecord + hhead.t_hoff + 4, datalen);
#if PG_VERSION_NUM >= 80300
	memcpy(&nullBitMap, (bits8 *) xlrecord + SizeOfHeapUpdate + SizeOfHeapHeader, BITMAPLEN(HeapTupleHeaderGetNatts(&hhead)) * sizeof(bits8));
#else
#warning "Copying null bitmap is not implemented for 8.2.x." /* FIXME: need to fix for 8.2 */
#endif

	printf("UPDATE \"%s\" SET ", relName);

	// Get relation field names and types
	if (oid2name_enabled())
	{
		int	i, rows = 0, fieldSize = 0;
		
		rows = relid2attr_begin(relName);

		offset = 0;

		for(i = 0; i < rows; i++)
		{
			char attname[NAMEDATALEN];
			Oid atttypid;

			relid2attr_fetch(i, attname, &atttypid);

			printf("%s%s = ", (i == 0 ? "" : ", "), attname);

			/* is the attribute value null? */
			if((hhead.t_infomask & HEAP_HASNULL) && (att_isnull(i, nullBitMap)))
			{
				printf("NULL");
			}
			else
			{
				printf("'");
				if(!(fieldSize = printField(data, offset, atttypid, datalen)))
					break;

				printf("'");
				offset += fieldSize;
			}
		}
		printf(" WHERE ... ;\n");
	}
}

/*
 * Print the field based on a chunk of xlog data and the field type
 * The maxfield len is just for error detection on variable length data,
 * actualy is based on the xlog record total lenght
 */
static int
printField(char *data, int offset, int type, uint32 maxFieldLen)
{
	int32 i, size;
	int64 bigint;
	int16 smallint;
	float4 floatNumber;
	float8 doubleNumber;
	Oid objectId;
	
	// Just print out the value of a specific data type from the data array
	switch(type)
	{
		case 700: //float4
			memcpy(&floatNumber, &data[offset], sizeof(float4));
			printf("%f", floatNumber);
			return sizeof(float4);
		case 701: //float8
			memcpy(&doubleNumber, &data[offset], sizeof(float8));
			printf("%f", doubleNumber);
			return sizeof(float8);
		case 16: //boolean
			printf("%c", (data[offset] == 0 ? 'f' : 't'));
			return MAXALIGN(sizeof(bool));
		case 1043: //varchar
		case 1042: //bpchar
		case 25: //text
		case 18: //char
			memcpy(&size, &data[offset], sizeof(int32));
			//@todo usar putc
			if(size > maxFieldLen || size < 0)
			{
				fprintf(stderr, "ERROR: Invalid field size\n");
				return 0;
			}
			for(i = sizeof(int32); i < size; i++)
				printf("%c", data[offset + i]);
				
			//return ( (size % sizeof(int)) ? size + sizeof(int) - (size % sizeof(int)):size);
			return MAXALIGN(size * sizeof(char));
		case 19: //name
			for(i = 0; i < NAMEDATALEN && data[offset + i] != '\0'; i++)
				printf("%c", data[offset + i]);
				
			return NAMEDATALEN;
		case 21: //smallint
			memcpy(&smallint, &data[offset], sizeof(int16));
			printf("%i", (int) smallint);
			return sizeof(int16);
		case 23: //int
			memcpy(&i, &data[offset], sizeof(int32));
			printf("%i", i);
			return sizeof(int32);
		case 26: //oid
			memcpy(&objectId, &data[offset], sizeof(Oid));
			printf("%i", (int) objectId);
			return sizeof(Oid);
		case 20: //bigint
			//@todo como imprimir int64?
			memcpy(&bigint, &data[offset], sizeof(int64));
			printf("%i", (int) bigint);
			return sizeof(int64);
		case 1005: //int2vector
			memcpy(&size, &data[offset], sizeof(int32));
			return MAXALIGN(size);
			
	}
	return 0;
}


