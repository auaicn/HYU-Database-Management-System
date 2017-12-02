#pragma once
#ifndef __BPT_H__
#define __BPT_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <inttypes.h>

#define true (1)
#define false (0)

#define page_size (4096)
#define header_page_size (128)
#define internal_key_pointer_size (16)
#define leaf_key_record_size (128)
#define HEADER_PAGE_OFFSET (0)

//struct declaration
typedef struct entry ENTRY; //for queue used in display.
typedef struct buffer_structure buffer_structure;
typedef struct buffer_manager buffer_manager;
typedef struct table_structure table_structure;
typedef struct table_manager table_manager;

extern char* null120;
extern int current_fd;
extern int current_table_id;
extern int64_t current_root_offset;
extern int64_t zero;
extern int table_create_count;

extern buffer_manager* BUFFER_MANAGER;
extern table_manager* TABLE_MANAGER;


//join operation
int join_table(int table1_id, int table2_id, const char* save_path);

// utility
void init();
void shutdown_db();
int open_table(char* path);
int test_leaf(buffer_structure* block);
int get_numKeys_of_page(buffer_structure* page);
int page_key_record_size(buffer_structure* page);
int get_order_of_current_page(buffer_structure* page);
int cut(int length);
void show_me_buffer();

//buffer_manager functions
void make_free_page();
void make_header_page();
buffer_structure* make_leaf_page();
buffer_structure* make_internal_page();

//find record & find leaf
char* find(int table_id, int64_t key);
buffer_structure* find_leaf(int table_id,int64_t key);

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

buffer_manager* new_buffer_manager(int max_blocks);
buffer_structure* new_buffer_block(int table_id, int64_t offset);
void new_table_manager();

void show_tables();
void init_db(int buffer_size);
void update(buffer_structure* block);
void bl_delete(buffer_structure * target);
buffer_structure* ask_buffer_manager(int table_id, int64_t offset);

//struct definition
struct entry {

	buffer_structure* page;
	int depth;

};

struct buffer_structure {
	
	char * frame;
	int table_id;
	int is_dirty;
	int pin_count;
	int64_t page_offset;

	buffer_structure* next;
	buffer_structure* prev;

};

struct buffer_manager {

	int num_current_blocks;
	int num_max_blocks;

	buffer_structure* MRU;
	buffer_structure* LRU;

};

struct table_structure 
{

	int fd;
	int table_id;
	char path[120];
	int root_offset;

	table_structure* next;

};

struct table_manager {
	table_structure* tables;
};

#endif /* __BPT_H__ */