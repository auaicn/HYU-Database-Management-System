#ifndef __BPT_H__ 
#define __BPT_H__ 

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <limits.h>

#define true (1)
#define false (0)

#define PageLSNAbsoluteOffset (24)
#define page_size (4096)
#define header_page_size (128)
#define internal_key_pointer_size (16)
#define leaf_key_record_size (128)
#define HEADER_PAGE_OFFSET (0)

#define BEGIN (int) 0
#define UPDATE (int) 1
#define COMMIT (int) 2 
#define ABORT (int) 3
#define CLR (int) 4

//set by append_to_transaction()
#define LSN 0
#define Prev_LSN 8
#define transaction_ID 16

//set by new_log()
#define Type 20

//set only by update()
#define Table_ID 24
#define Page_Number 28
#define OFFSET 32
#define Data_Length 36
#define Old_Image 40
#define New_Image 160

#define PAGE_LSN 24
#define LOG_SIZE 300

//struct declaration
typedef struct entry ENTRY; //for queue used in display.
typedef struct buffer_structure buffer_structure;
typedef struct buffer_manager buffer_manager;
typedef struct table_structure table_structure;
typedef struct table_manager table_manager;
typedef struct log log;
typedef struct log_manager log_manager;
typedef struct transactional_unit transactional_unit;

//global vars
extern char* null120;
extern int current_fd;
extern int current_table_id;
extern int64_t current_root_offset;
extern int64_t zero;
extern int table_create_count;
extern int transaction_count;
extern int need_recovery;

extern buffer_manager* BUFFER_MANAGER;
extern table_manager* TABLE_MANAGER;
extern log_manager* LOG_MANAGER;

//project recovery interface
void new_transactional_unit();
log* new_log(int type);
void new_log_manager();
log* read_one_log();
int flush_transaction();
int remove_transaction();
void append_to_transaction(log* new_log);

int display_current_transaction_log_on_memory();
void recovery();
int begin_transaction();
int commit_transaction();
int abort_transaction();
int update(int table_id, int64_t key, char* value);

void setter_leaf_value(char*frame, int index, char* new_value);

//changed interface
void flush_page(buffer_structure* block);
buffer_structure* fetch(int table_id, int64_t offset);

//join operation
int join_table(int table1_id, int table2_id, const char* save_path);
table_structure* is_opened(int table_id);

//utility for recovery
int64_t getter_log_value_8byte(log* single_log, int offset);
int getter_log_value_4byte(log* single_log, int offset);

// utility
void init();
void usage();
table_structure* get_table_structure(int table_id);
table_structure* get_table_structure_by_name(char* pathname);

void shutdown_db();
int open_table(char* path);
int test_leaf(buffer_structure* block);
int get_numKeys_of_page(buffer_structure* page);
int page_key_record_size(buffer_structure* page);
int get_order_of_current_page(buffer_structure* page);
int cut(int length);
void show_me_buffer();
int64_t getter_internal_key(char* frame, int index);
int64_t getter_leaf_key(char* frame, int index);
char* getter_leaf_value(char* frame, int index);

//buffer_manager functions
void make_free_page();
void make_header_page(int table_id);
buffer_structure* make_leaf_page();
buffer_structure* make_internal_page();

//find record & find leaf
char* find(int table_id, int64_t key);
buffer_structure* find_leaf(int table_id, int64_t key);

//insert
int insert(int table_id, int64_t key, char* value);
int insert_into_leaf(buffer_structure* leaf, int64_t key, char* value);
int insert_into_leaf_after_splitting(buffer_structure* left_offset, int64_t key, char* value);
int insert_into_parent(buffer_structure* left, buffer_structure* right, int64_t new_key);
int insert_into_new_root(buffer_structure* left, int64_t key, buffer_structure* right); //have to return or change root
int insert_into_node(buffer_structure* parent, int left_index, int64_t new_key, buffer_structure* right);
int insert_into_node_after_splitting(buffer_structure* parent, int left_index, int64_t new_key, buffer_structure
	* right_child);

//delete
int delete(int table_id, int64_t key);
int get_neighbor_index(buffer_structure* parent, buffer_structure* page);
int remove_entry_from_page(buffer_structure* page, int64_t key);
void return_to_free_page(buffer_structure* page);
int delete_entry(buffer_structure* page, int64_t key);
int redistribute_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page, int neighbor_index, int prime_key_index, int64_t prime_key);
int coalesce_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page, int neighbor_index, int64_t prime_key);
void display(int table_id);

void new_buffer_manager(int max_blocks);
buffer_structure* new_buffer_block(int table_id, int64_t offset);
void new_table_manager();

void close_table(int table_id);
void show_tables();
void init_db(int buffer_size);
void bl_delete(buffer_structure * target);

/*struct definition*/

struct log_manager {
	int64_t flushed_LSN;
	FILE* log_file;
	//initialized by new_log_manager (that is called in init_db())
	transactional_unit* current_transaction;
};

struct transactional_unit {
	int64_t lastLSN;
	log* begin;
	log* end;
	int XID;
};

//it indicates log object on memory
//volatile
struct log {
	char* frame;
	log* next;
	log* prev;
};

struct entry {
	buffer_structure* page;
	int depth;

};

struct buffer_structure {

	char * frame;
	//24-32 PAGE LSN
	int table_id;
	int is_dirty;
	int pin_count;
	int64_t page_offset;
	int64_t page_LSN;

	buffer_structure* next;
	buffer_structure* prev;

};

struct buffer_manager {

	int num_current_blocks;
	int num_max_blocks;

	buffer_structure* header;

};

struct table_structure
{

	int fd;
	int table_id;
	char path[120];
	int64_t root_offset;

	table_structure* next;

};

struct table_manager {
	table_structure* tables;
};

#endif /* __BPT_H__ */