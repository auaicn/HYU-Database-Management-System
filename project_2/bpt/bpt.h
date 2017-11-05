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
int64_t root = 0;//루트는 계속 가지고 있으면서 갱신해주자.
int fd;//이 값은 open command시에만 바뀔 수 있다.

char char_null[120];
int64_t bit64_0 = 0;

//branch factor ( order ) 
//leaf : 32 (numKeys will be 31)
//internal : 249 (numKeys will be 248)

//useful functions for object oriented programming
//added when refactoring
int64_t page_key_record_size(int64_t page_offset);
int64_t value_read(int64_t offset, int byte); //pread and then returns what is written. used for getting key.
ssize_t swrite(int fd, const void* buf, int64_t size_, int64_t offset);
void shift_byte(int64_t start, int64_t end, int64_t size_);
int get_order_of_current_page(int64_t page_offset);
int get_numKeys_of_page(int64_t page_offset);
int cut(int length);
int test_leaf(int64_t page_offset);

//free_page_management
void make_free_page(); //free page를 만드는 것이다. make_header_page에서 한번 부른다.
void make_header_page();
int64_t make_leaf_page(); //to use offset
int64_t make_internal_page();

//open_db
int open_db(const char*); //가장 처음에 부르는 것이다.

//find record & find leaf
char* find(int64_t key);
int64_t find_leaf(int64_t key);

//insert
int insert(int64_t key, char* value);
void insert_into_leaf(int64_t leaf_offset, int64_t key, char* value);
void insert_into_leaf_after_splitting(int64_t left_offset, int64_t key, char* value);
void insert_into_parent(int64_t left, int64_t right, int64_t new_key);
void insert_into_new_root(int64_t left, int64_t key, int64_t right); //have to return or change root
void insert_into_node(int left_index, int64_t parent, int64_t new_key, int64_t right);
void insert_into_node_after_splitting(int left_index, int64_t new_key, int64_t right);

//delete
int delete_(int64_t key);
int get_neighbor_index(int64_t offset);
void remove_entry_from_page(int64_t key, int64_t offset);
void return_to_free_page(int64_t offset);
void delete_entry(int64_t key_leaf, int64_t key);
void redistribute_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int prime_key_index, int64_t prime_key);
void coalesce_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int64_t prime_key);
void display();

typedef struct entry{
	int64_t offset;
	int depth;
}ENTRY;