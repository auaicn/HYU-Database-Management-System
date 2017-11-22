#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>

#define true (1)
#define false (0)
#define page_size (4096)
#define header_page_size (128)
#define key_record_size (128)
#define HEADER_PAGE_OFFSET (0)
#define FREE_PAGE_LIST_OFFSET (4096)

//struct declaration
typedef struct entry ENTRY; //for queue used in display.
typedef struct buffer_structure buffer_structure;
typedef struct buffer_manager buffer_manager;

extern int64_t root;//루트는 계속 가지고 있으면서 갱신해주자.
extern int fd; //initially 0
extern char char_null[120];
extern int64_t bit64_0;
extern int table_count;
extern buffer_manager* BUFFER_MANAGER;

// utility
int test_leaf(buffer_structure* block);
int get_numKeys_of_page(buffer_structure* page);
int page_key_record_size(buffer_structure* page);
int get_order_of_current_page(buffer_structure* page);
int cut(int length);
void show_me_buffer();
//free_page_management & open db
void make_free_page();
void make_header_page();
buffer_structure* make_leaf_page();
buffer_structure* make_internal_page();
int open_db(const char* path);

/*
branch factor ( order )
leaf : 32 (numKeys will be 31)
internal : 249 (numKeys will be 248)
*/

//find record & find leaf
char* find(int table_id, int64_t key);
buffer_structure* find_leaf(int64_t key);

//insert
int insert(int table_id,int64_t key, char* value);
void insert_into_leaf(buffer_structure* leaf, int64_t key, char* value);
void insert_into_leaf_after_splitting(buffer_structure* left_offset, int64_t key, char* value);
void insert_into_parent(buffer_structure* left, buffer_structure* right, int64_t new_key);
void insert_into_new_root(buffer_structure* left, int64_t key, buffer_structure* right); //have to return or change root
void insert_into_node(int left_index, buffer_structure* parent, int64_t new_key, buffer_structure* right);
void insert_into_node_after_splitting(buffer_structure* left, int left_index, int64_t new_key,buffer_structure
	* right_child);

//delete
int delete_(int table_id, int64_t key);
int get_neighbor_index(buffer_structure* parent, buffer_structure* page);
int remove_entry_from_page(buffer_structure* page, int64_t key);
void return_to_free_page(buffer_structure* page);
int delete_entry(buffer_structure* page, int64_t key);
int redistribute_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page, int neighbor_index, int prime_key_index, int64_t prime_key);
int coalesce_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page, int neighbor_index, int64_t prime_key);
void display();

buffer_manager* new_buffer_manager(int max_blocks);
buffer_structure* new_buffer_block(int64_t offset);

int get_table_id();
void close_db(int table_id_);
void init_db(int buffer_size);
void update(buffer_structure* block);
buffer_structure* ask_buffer_manager(int64_t offset);

//struct definition
struct entry {
	buffer_structure* page;
	int depth;
};

struct buffer_structure {
	char * frame;
	int table_id;
	int64_t page_offset;
	int is_dirty;
	int pin_count;
	buffer_structure* next;
	buffer_structure* prev;
};

struct buffer_manager {
	int num_current_blocks;
	int max_blocks;
	buffer_structure* MRU;
	buffer_structure* LRU;
};

#endif /* __BPT_H__ */
