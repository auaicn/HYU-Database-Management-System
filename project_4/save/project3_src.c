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

extern int64_t root;//루트는 계속 가지고 있으면서 갱신해주자.
extern int fd; //initially 0
extern char char_null[120];
extern int64_t bit64_0;
extern int table_count;

// utility
int64_t page_key_record_size(int64_t page_offset);
int64_t value_read(int64_t offset, int byte);
int get_order_of_current_page(buffer_structure* page);
int get_numKeys_of_page(buffer_structure* page);
int cut(int length);
int test_leaf(buffer_structure* block);

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
void insert_into_node_after_splitting(buffer_structure* parent, int left_index, int64_t new_key, buffer_structure* right);

//delete
int delete_(int table_id, int64_t key);
int get_neighbor_index(int64_t offset);
void remove_entry_from_page(int64_t key, int64_t offset);
void return_to_free_page(int64_t offset);
void delete_entry(int64_t key_leaf, int64_t key);
void redistribute_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int prime_key_index, int64_t prime_key);
void coalesce_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int64_t prime_key);
void display();


typedef struct entry ENTRY;
typedef struct buffer_structure buffer_structure;
typedef struct buffer_manager buffer_manager;

buffer_manager* new_buffer_manager(int max_blocks);
buffer_structure* new_buffer_block(int64_t offset);

int get_table_id();
void close_db(int table_id_);
void init_db(int buffer_size);
void update(buffer_structure* block);
buffer_structure* ask_buffer_manager(int64_t offset);

//struct definition
struct entry {
	int64_t offset;
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

buffer_manager* BUFFER_MANAGER;
#endif

//struct constructor
buffer_manager* new_buffer_manager(int max_blocks) {
	buffer_manager* new_buffer_manager_ = (buffer_manager*)malloc(sizeof(buffer_manager));
	new_buffer_manager_->LRU = NULL;
	new_buffer_manager_->MRU = NULL;
	new_buffer_manager_->max_blocks = max_blocks;
	new_buffer_manager_->num_current_blocks = 0;
	return new_buffer_manager_;
}

buffer_structure* new_buffer_block(int64_t offset) {
	buffer_structure* new_buffer = (buffer_structure*)malloc(sizeof(buffer_structure));
	new_buffer->frame = (char*)malloc(page_size);
	pread(fd, new_buffer->frame, 4096, offset);// pread only here
	new_buffer->next = NULL;
	new_buffer->prev = NULL;
	new_buffer->table_id = get_table_id();
	new_buffer->is_dirty = 0;
	new_buffer->pin_count = 0;
	new_buffer->page_offset = offset;
	return new_buffer;
}

//definitions using struct


//normal definitions


int get_table_id() {
	return table_count;
}


buffer_structure* ask_buffer_manager(int64_t offset) {

	buffer_structure* search = BUFFER_MANAGER->MRU;
	if (search == NULL) {
		//first ask_buffer
		//MRU NULL
		buffer_structure* first_buffer_block = new_buffer_block(offset);
		BUFFER_MANAGER->MRU = first_buffer_block;
		BUFFER_MANAGER->LRU = first_buffer_block;
		first_buffer_block->next = NULL;
		first_buffer_block->prev = NULL;
		return first_buffer_block;
	}

	//MRU NOT NULL
	//at least one element in the list
	for (; search != NULL; search = search->next) {
		if (memcmp(&search->page_offset, &offset, 8)) {
			//cache hit search.offset == offset
			//chacge location in list
			//remove
			if (BUFFER_MANAGER->MRU == search)
				return search;
			if (search->next == NULL) {
				search->prev->next = search->next; //RVALUE is NULL
				BUFFER_MANAGER->LRU = search->prev;
			}
			else {
				search->next->prev = search->prev;
				search->prev->next = search->next;
			}

			//insert
			search->next = BUFFER_MANAGER->MRU;
			search->prev = search->next->prev; //RVALUE is NULL
			search->next->prev = search;
			BUFFER_MANAGER->MRU = search;

			return search;
		}
	}
	//cache not hit 
	//MRU NOT NULL
	//at least one element in the list
	if (search == NULL) {
		//search has no meaning from now on
		buffer_structure* new_block = new_buffer_block(offset);

		new_block->next = BUFFER_MANAGER->MRU;
		new_block->prev = new_block->next->prev; //R VALUE is NULL
		new_block->next->prev = new_block;
		BUFFER_MANAGER->MRU = new_block;

		//remove into list
		if (BUFFER_MANAGER->num_current_blocks == BUFFER_MANAGER->max_blocks) {
			//buffer full. need victim
			//find victim. need not but just implemented.
			//victime은 LRU 부터 search
			buffer_structure* victim = BUFFER_MANAGER->LRU; //Never be NULL but it's prev can be NULL
			while (victim->pin_count != 0)
				victim = victim->prev;

			//insert into list
			//if concurrency control implement, flow has to be controlled.
			BUFFER_MANAGER->LRU = victim->prev;
			BUFFER_MANAGER->LRU->next = victim->next; //R VALUE is NULL

													  //check victom
			if (victim->is_dirty == true) {
				update(BUFFER_MANAGER->LRU); fdatasync(fd);
			}
		}
		else {
			//buffer not full
			BUFFER_MANAGER->num_current_blocks++;
		}
		return new_block;
	}

}


void close_db(int table_id_) {
	buffer_structure* ptr;
	for (ptr = BUFFER_MANAGER->MRU; ptr != NULL; ptr = ptr->next) {
		buffer_structure* current_block_ptr = ptr;
		if (ptr->table_id == table_id_) {

			//unlink
			if (current_block_ptr->prev == NULL)
				BUFFER_MANAGER->MRU = current_block_ptr->next;
			else
				current_block_ptr->prev->next = current_block_ptr->next;
			if (current_block_ptr->next == NULL)
				BUFFER_MANAGER->LRU = current_block_ptr->prev;
			else
				current_block_ptr->prev->next = current_block_ptr->next;

			//update and then free heap
			update(current_block_ptr);
			free(current_block_ptr);
			(BUFFER_MANAGER->num_current_blocks)--;
		}

	}
	fdatasync(fd);
	return;
}

void init_db(int buffer_size) {
	//initialize BUFFER_MANAGER
	BUFFER_MANAGER = (buffer_manager*) malloc(sizeof(buffer_manager));
	BUFFER_MANAGER->max_blocks = buffer_size;
	BUFFER_MANAGER->MRU = NULL;
	BUFFER_MANAGER->LRU = NULL;
}

void update(buffer_structure* block) {
	pwrite(fd, block->frame, 4096, block->page_offset);
}

void display() {
	
	if (root == 0) {
		printf("empty tree.\n");
		return;
	}

	int64_t numpages; pread(fd, &numpages, 8, 16);
	ENTRY *queue = (ENTRY*)calloc(numpages, sizeof(struct entry));

	int64_t queue_front = 0;
	int64_t queue_end = -1;
	ENTRY root_;
	root_.offset = root;
	root_.depth = 0;
	queue[++queue_end] = root_; //push root

	int depth_by_now = -1;
	while (queue_front <= queue_end) {
		//get front and then pop
		ENTRY pos = queue[queue_front++];
		if (depth_by_now != pos.depth) {
			if (depth_by_now != -1) printf("|\n");
			depth_by_now = pos.depth;
		}

		int numChild; pread(fd, &numChild, 4, 12 + pos.offset);
		//print key
		int key_record_size_ = test_leaf(pos.offset) ? 128 : 16;

		printf("(p.%" PRId64")", pos.offset / 4096);
		int i;
		printf("| ");
		for (i = 0; i<numChild; i++) {
			printf("%" PRId64" ", value_read(pos.offset + header_page_size + key_record_size_*i, 8));
		}

		//enqueue others
		//not for leaf
		if (test_leaf(pos.offset)) continue;
		//for internal
		for (i = 0; i<numChild + 1; i++) {
			++queue_end;
			queue[queue_end].offset = value_read(pos.offset + header_page_size - 8 + 16 * i, 8);
			queue[queue_end].depth = pos.depth + 1;
		}
	}

	printf("|\n");
	free(queue);
	return;
}

void return_to_free_page(int64_t offset) {
	//insert into the stack-implemented free_pages.
	// (stack) 
	int64_t temp; pread(fd, &temp, 8, 4096);
	pwrite(fd, &temp, 8, offset);
	pwrite(fd, &offset, 8, 4096);
	return;
}

void adjust_root() {
	//printf("adjusting root\n");
	int num; pread(fd, &num, 4, root + 12);
	if (num) return; //there's key in root -> no need to adjust.
	else {
		if (test_leaf(root)) { //make tree to be empty.

			return_to_free_page(root);
			pwrite(fd, &bit64_0, 8, 8);
			//no need to set parent of root cause it's null
			root = 0;
		}
		else {
			int64_t new_root; pread(fd, &new_root, 8, root + header_page_size - 8); //get first child.

			return_to_free_page(root); //free root and then record ne root.
			pwrite(fd, &new_root, 8, 8);
			pwrite(fd, &bit64_0, 8, new_root); //new root has no parent.
			root = new_root; //set global 
		}
	}
}


void coalesce_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int64_t prime_key) {
	//printf("coalescing!\n");
	int64_t parent; pread(fd, &parent, 8, page);
	int numKeys_neighbor;
	int numKeys_page;
	if (neighbor_index == -1) {
		int64_t temp = neighbor_page;
		neighbor_page = page;
		page = temp;
	}
	numKeys_neighbor = get_numKeys_of_page(neighbor_page);
	numKeys_page = get_numKeys_of_page(page);

	// now neighbor_page - page

	if (!test_leaf(page)) {
		//colaesce internal pages 
		int i;
		for (i = 0; i <= numKeys_page; i++) {
			int64_t child; pread(fd, &child, 8, page + header_page_size + 16 * i - 8);
			pwrite(fd, &neighbor_page, 8, child);
		}
		pwrite(fd, &prime_key, 8, neighbor_page + header_page_size + 16 * (numKeys_neighbor));
		numKeys_neighbor++;
		shift_byte(page + header_page_size - 8, neighbor_page + header_page_size - 8 + 16 * numKeys_neighbor, 16 * numKeys_neighbor + 8);
		numKeys_neighbor += 1; //important
	}
	else {
		//leaf 
		//neighbor - original
		shift_byte(page + header_page_size, neighbor_page + header_page_size + 128 * numKeys_neighbor, 128 * numKeys_page);
		shift_byte(page + header_page_size - 8, neighbor_page + header_page_size - 8, 8);

	}
	numKeys_neighbor += numKeys_page;
	delete_entry(parent, prime_key); //prime_key down.
	pwrite(fd, &numKeys_neighbor, 4, neighbor_page + 12); //renew merged 
	return_to_free_page(page);
	return;
}


int64_t page_key_record_size(int64_t page_offset) {
	if (test_leaf(page_offset))
		return 128;
	else
		return 16;
}


void redistribute_pages(int64_t page, int64_t neighbor_page, int neighbor_index, int prime_key_index, int64_t prime_key) {
	//printf("redistributing!\n");
	int64_t parent; pread(fd, &parent, 8, page);
	int64_t new_key;

	int numKeys_parent; pread(fd, &numKeys_parent, 4, 12 + parent);
	int numKeys_neighbor; pread(fd, &numKeys_neighbor, 4, 12 + neighbor_page);
	int numKeys_page; pread(fd, &numKeys_page, 4, 12 + page);

	// neighbor is at left
	if (neighbor_index != -1) {
		//shifting byte
		if (!test_leaf(page))
			//internal page
			//shift by 16
			shift_byte(page + header_page_size - 8, page + header_page_size + 8, 16 * get_numKeys_of_page(page) + 8);
		else
			//leaf page
			//shift by 128
			shift_byte(page + header_page_size, page + header_page_size + 128, 128 * get_numKeys_of_page(page));
		//filling byte
		if (!test_leaf(page)) {
			//internal page
			int64_t temp_key;
			pread(fd, &temp_key, 8, neighbor_page + header_page_size + 16 * (numKeys_neighbor - 1));
			pwrite(fd, &temp_key, 8, page + header_page_size);
			//just copied and then not recorded into parent
			int64_t temp_offset;
			pread(fd, &temp_offset, 8, neighbor_page + header_page_size + 16 * (numKeys_neighbor - 1) + 8);
			pwrite(fd, &temp_key, 8, page + header_page_size - 8);

		}
		else { //leaf
			shift_byte(neighbor_index + header_page_size + 128 * (numKeys_neighbor - 1), page + header_page_size, 128);
		}

		pread(fd, &new_key, 8, page + header_page_size);
		pwrite(fd, &new_key, 8, parent + header_page_size + prime_key_index*page_key_record_size(page));

	}

	// neighbor is at right

	else {

		//filling byte
		//internal page
		if (!test_leaf(page)) {
			int64_t temp_key;
			pread(fd, &temp_key, 8, neighbor_page + header_page_size);
			pwrite(fd, &temp_key, 8, page + header_page_size + 16 * numKeys_page);
			int64_t temp_offset;
			pread(fd, &temp_offset, 8, neighbor_page + header_page_size - 8);
			pwrite(fd, &temp_offset, 8, page + header_page_size + 16 * numKeys_page + 8);
		}
		else
			shift_byte(neighbor_page + header_page_size, page + header_page_size + 128 * numKeys_page, 128);

		if (!test_leaf(page))
			//internal page
			//shift by 16
			shift_byte(neighbor_page + header_page_size + 8, neighbor_page + header_page_size - 8, 16 * (numKeys_neighbor - 1) + 8);
		else
			//leaf page
			//shift by 128
			shift_byte(neighbor_page + header_page_size + 128, neighbor_page + header_page_size, 128 * (numKeys_neighbor - 1));
		pread(fd, &new_key, 8, neighbor_page + header_page_size);
		pwrite(fd, &new_key, 8, parent + header_page_size + prime_key_index*page_key_record_size(parent));


	}

	numKeys_neighbor--; pwrite(fd, &numKeys_neighbor, 4, 12 + neighbor_page);
	numKeys_page++; pwrite(fd, &numKeys_page, 4, 12 + page);

}

int get_neighbor_index(int64_t offset) {
	int64_t parent; pread(fd, &parent, 8, offset);

	int i = 0;
	int64_t to_compare;
	while (i < get_numKeys_of_page(parent)) {
		//scan [120-128] [136-144] ...
		pread(fd, &to_compare, 8, parent + header_page_size - 8 + 16 * i);
		if (to_compare == offset) break;
		i++;
	}

	return i - 1;
}

void delete_entry(int64_t page, int64_t key) {

	remove_entry_from_page(key, page);

	if (page == root) //deletion at root -> maybe root has to be disappeared.
		return adjust_root();

	int min_keys = get_order_of_current_page(page) / 2;

	if (get_numKeys_of_page(page) >= min_keys) {
		//printf("delete finished (enough numKeys)\n");
		return;
	}
	//get parent of page
	int64_t parent; pread(fd, &parent, 8, page);
	//get neighbor_index & prime_key & numKeys
	int neighbor_index = get_neighbor_index(page); // [0-numKeys] not [-1 to numKeys-1];
	int prime_key_index = neighbor_index == -1 ? 0 : neighbor_index;// [0-(numKeys -1)]
	int temp = (neighbor_index == -1) ? 1 : neighbor_index;

	int64_t neighbor;
	pread(fd, &neighbor, 8, temp * 16 + parent + header_page_size - 8);

	int64_t prime_key; pread(fd, &prime_key, 8, 16 * prime_key_index + header_page_size + parent);
	int capacity = get_order_of_current_page(page) - 1;
	// 31 for leaf and 248 for internal

	if (get_numKeys_of_page(neighbor) + get_numKeys_of_page(page) <= capacity)
		return coalesce_pages(page, neighbor, neighbor_index, prime_key);
	else
		return redistribute_pages(page, neighbor, neighbor_index, prime_key_index, prime_key);

}

int delete_(int table_id, int64_t key) {
	
	char* key_record = find(get_table_id(), key);
	buffer_structure* leaf = find_leaf(key);

	if (key_record != 0 && leaf !=NULL) {
		delete_entry(leaf, key);
		free(key_record);
	}
	else {
		printf("there is no record that matches with the key (%" PRId64").\n",key);
	}
	return 0;
}

void remove_entry_from_page(int64_t key, int64_t offset) {
	int i = 0;
	int64_t key_pointer_or_record_size = test_leaf(offset) ? key_record_size : 16;
	int numKeys = get_numKeys_of_page(offset);

	int64_t to_compare;
	while (i < numKeys) {
		pread(fd, &to_compare, 8, offset + header_page_size + i*key_pointer_or_record_size);
		if (to_compare == key) break;
		else i++;
	}
	if (to_compare != key) {
		//printf("not found sorry. T^T\n");
		return;
	}

	//key found
	shift_byte(offset + header_page_size + (i + 1)*key_pointer_or_record_size, offset + header_page_size + i*key_pointer_or_record_size, (numKeys - 1 - i)*key_pointer_or_record_size);

	numKeys--;
	pwrite(fd, &numKeys, 4, offset + 12);

	//maybe we can set the other pointers and keys to null for tidiness

}

void insert_into_node_after_splitting(buffer_structure* left, int left_index, int64_t new_key, buffer_structure* right) {
	//printf("inserting into new internal page.\n");
	int split;
	//get parent from right child.
	int order_ = get_order_of_current_page(left);
	//make new internal page.
	buffer_structure* right = make_internal_page();
	//printf("new internal_page created : p.%" PRId64"\n", new_internal_page_offset/4096);
	char* temp = (char*)malloc(order_ * 16);

	//copy to temp
	memcpy(temp, left->frame + header_page_size, 16 * left_index);
	memcpy(temp + left_index * 16, &new_key, 8);
	memcpy(temp + left_index * 16 + 8, &right, 8);
	memcpy(temp + 16 * (left_index + 1)
		, left->frame + header_page_size + 16 * left_index
		, 16 * (order_ - 1 - left_index));

	split = cut(order_);

	//renew numKeys
	int numKeys_left = split - 1; memcpy(left->frame + 12, &numKeys_left, 4);
	int numKeys_right = order_ - (split - 1) - 1; memcpy(right->frame + 12, &numKeys_right, 4);
	
	memcpy(right->frame + header_page_size - 8
		, temp + split * 16 - 8
		, 16 * (order_ - split) + 8);

	//extract new_key
	int64_t prime_key;
	memcpy(&prime_key, left->frame + header_page_size + 16 * (split - 1), 8);

	//new_internal_page에 있는 child 들의 부모를 new_internal_page로 바꿔주어야 한다.
	int i;
	for (i = 0; i < numKeys_right + 1; i++) {

		int64_t child_offset; memcpy(&child_offset, right->frame + header_page_size - 8 + 16 * i, 8);
		buffer_structure* child = ask_buffer_manager(child_offset);
		memcpy(child->frame, &right->page_offset, 8);
	}
	
	//set right's parent
	int64_t parent_of_left; memcpy(&parent_of_left, left->frame, 8);
	memcpy(right->frame, &parent_of_left, 8);

	free(temp);

	insert_into_parent(left, right, prime_key);
}

void insert_into_node(int left_index, buffer_structure* parent,
	int64_t new_key, buffer_structure* right) {
	
	//no overflow then just insert it.

	int64_t absolute_insertion_offset = header_page_size + 16 * left_index;

	int numKeys; 
	memcpy(&numKeys, parent->frame + 12, 4);
	
	memmove(parent->frame + absolute_insertion_offset + 16
		, parent->frame + absolute_insertion_offset
		, 16 * (numKeys - left_index));

	memcpy(parent->frame + absolute_insertion_offset, &new_key, 8);
	memcpy(parent->frame + absolute_insertion_offset + 8, &right->page_offset, 8);

	numKeys++; 
	memcpy(parent->frame + 12, &numKeys, 4);
}


void insert_into_new_root(buffer_structure* left, int64_t key, buffer_structure* right) {

	buffer_structure* new_root = make_internal_page();
	//renew number of keys
	int number_of_keys;
	memcpy(&number_of_keys, new_root->frame + 12, 4);
	number_of_keys++; 
	memcpy(new_root->frame + 12, &number_of_keys, 4);

	//set parent offset to 0
	memcpy(new_root->frame, &bit64_0, 8);
	memcpy(left->frame, &new_root->page_offset, 8);
	memcpy(right->frame, &new_root->page_offset, 8);

	//set key, pointer 
	memcpy(new_root->frame + 120, &left->page_offset, 8);
	memcpy(new_root->frame + 128, &key, 8);
	memcpy(new_root->frame + 136, &right->page_offset, 8);

	//renew global root.
	root = new_root->page_offset;
	buffer_structure* header = ask_buffer_manager(HEADER_PAGE_OFFSET);
	memcpy(header->frame + 8, &root, 8);
	//printf("new root created. pageno is %" PRId64"\n", root/4096);
}


void insert_into_parent(buffer_structure* left, buffer_structure* right, int64_t new_key) {
	//from splitting. they both passes left and right child offset and new_key
	//offset of parent
	int64_t parent_offset; memcpy(&parent_offset, left->frame, 8);
	buffer_structure* parent = ask_buffer_manager(parent_offset);

	if (parent_offset == 0) {
		insert_into_new_root(left, new_key, right);
		return;
	}

	int left_index = 0;

	int64_t to_compare;
	while (left_index < get_numKeys_of_page(parent)) {

		memcpy(&to_compare, parent->frame + header_page_size + 16 * left_index, 8);

		if (new_key > to_compare) 
			left_index++;
		else 
			break;

	}
	if (get_numKeys_of_page(parent) < get_order_of_current_page(parent) - 1)
		insert_into_node(left_index, parent, new_key, right);
	else 
		insert_into_node_after_splitting(parent, left_index, new_key, right); 
	
	// #key already 248, become 249 then splitted #order = 249
}


void insert_into_leaf_after_splitting(buffer_structure* leaf, int64_t key, char* value) {
	
	int insertion_point = 0;
	int order_ = get_order_of_current_page(leaf);
	char* temp = (char*)calloc(order_, 128);
	int split = cut(order_);

	buffer_structure* new_leaf = make_leaf_page();
	memcpy(new_leaf->frame, leaf->frame, 8); //parent

	// leaf - new_leaf

	int64_t to_compare;
	while (insertion_point < order_ - 1) {

		memcpy(&to_compare, leaf->frame + header_page_size + insertion_point * 128, 8);

		if (key > to_compare) 
			insertion_point++;
		else 
			break;

	}

	//copy to temp
	memcpy(temp, leaf->frame + header_page_size, insertion_point*key_record_size);
	memcpy(temp + insertion_point*key_record_size, &key, 8);
	memcpy(temp + insertion_point*key_record_size + 8, value, 120);
	memcpy(temp + (insertion_point + 1)*key_record_size
		, leaf->frame + header_page_size + insertion_point*key_record_size
		, (order_ - 1 - insertion_point)*key_record_size);

	//temp to each node left & right
	memcpy(leaf->frame + header_page_size, temp, key_record_size*split);
	memcpy(new_leaf->frame + header_page_size
		, temp + key_record_size*split
		, key_record_size*(order_ - split));

	//numKeys left & right
	memcpy(leaf->frame + 12, &split, 4);
	int num_right_key = order_ - split;
	memcpy(new_leaf->frame + 12, &num_right_key, 4);

	//siblings control
	memcpy(new_leaf->frame + header_page_size - 8, leaf->frame + header_page_size - 8, 8);
	memcpy(leaf->frame + header_page_size - 8, &new_leaf->page_offset, 8);

	//new key is selected and send to the parent for high-level insert
	int64_t new_key; memcpy(&new_key, new_leaf->frame + header_page_size, 8);

	//free memory and call subroutine.
	free(temp);

	insert_into_parent(leaf, new_leaf, new_key);
}

int64_t value_read(int64_t offset, int byte_) {
	int64_t temp; pread(fd, &temp, byte_, offset);
	return temp;
}

void insert_into_leaf(buffer_structure* leaf, int64_t key, char* value) {
	//printf("pushing into leaf : p.%" PRId64 "\n", leaf_offset/4096);
	
	int insertion_point = 0;
	int num = get_numKeys_of_page(leaf);
	
	int64_t to_compare;
	while (insertion_point<num) {
		
		memcpy(&to_compare, leaf->frame + header_page_size + 128 * insertion_point, 8);
		
		if (to_compare < key) 
			insertion_point++;
		else 
			break;
	}

	int absolute_insertion_offset = header_page_size + insertion_point * 128;

	memmove(leaf->frame + absolute_insertion_offset+128,
		leaf->frame + absolute_insertion_offset,
		128 * (num - insertion_point));

	num++; memcpy(leaf->frame + 12, &num, 4);

	memcpy(leaf->frame + absolute_insertion_offset, &key, 8);
	memcpy(leaf->frame + absolute_insertion_offset + 8, char_null, 120);
	memcpy(leaf->frame + absolute_insertion_offset + 8, value, 120);

	leaf->is_dirty = true;

}

int insert(int table_id, int64_t key, char* value) { //사실 root가 전역변수라서 return 해줘야 하는지도 모르겠다.
	if (find(key) != 0) {
		//printf("duplicate insert.\ntry again.\n");
		return 0;
	}
	if (root == 0) { //first insert
		buffer_structure* root_ = make_leaf_page(); //안에서 할당이 되어서 나온다.
		buffer_structure* header_page = ask_buffer_manager(HEADER_PAGE_OFFSET);
		memcpy(header_page->frame + 8, &root_->page_offset, 8);
		//now we can access 8-16
		//한번은 직접 대입해주자
		//first key, first value
		memcpy(root_->frame + header_page_size, &key, 8);
		memcpy(root_->frame + header_page_size + 8, value, 120);
		memcpy(root_->frame, &bit64_0, 8);
		
		int numKeys; memcpy(&numKeys, root_->frame + 12, 4);
		numKeys++; memcpy(root_->frame + 12, &numKeys, 4);

		root = root_->page_offset;
		return 0;
	}
	buffer_structure* leaf = find_leaf(key);
	int max_numKeys = get_order_of_current_page(leaf) - 1;

	if (get_numKeys_of_page(leaf) < max_numKeys) {
		//printf("%d < %d -> insert_into_leaf\n", get_numKeys_of_page(leaf), max_numKeys);
		insert_into_leaf(leaf, key, value);
		return 0;
	}
	insert_into_leaf_after_splitting(leaf, key, value); //why root not global variable?
	return 0;
}

int get_numKeys_of_page(buffer_structure* page){
	int temp; 
	memcpy(&temp, page->frame + 12, 4);
	return temp;
}

int cut(int numChildrens) {
	//ceiling to the float point of half the argument.
	//returns 3 for 6
	//returns 4 for 7 always left one is bigger
	if (numChildrens % 2 == 0)
		return numChildrens / 2;
	else
		return numChildrens / 2 + 1;
}

int test_leaf(buffer_structure* block) {
	int is_leaf;
	memcpy(&is_leaf, block->frame + 8, 4);
	if (is_leaf) return true;
	else return false;
	//leaf확인이 필요하면 나중에 또 부르거나 저장해서 쓰면 되겠지.
}


int get_order_of_current_page(buffer_structure* page) {
	//branch factor (order)
	//leaf : 32
	//internal : 249
	//invariant : numKeys is 1-less than order
	if (test_leaf(page))
		return 32;
	else
		return 249;
}

buffer_structure* find_leaf(int64_t key) {

	if (root == 0) {//root offset initialized to 0
		printf("empty tree\n");
		return NULL; //empty tree : return 0 to find function
	}

	//ask for root buffer_structure
	int64_t search_offset = root;
	buffer_structure* search = ask_buffer_manager(search_offset);
	while (true) {//linear search
		if (test_leaf(search)) {
			break;
		}
		//not leaf
		int number_of_keys;
		memcpy(&number_of_keys, search->frame + 12, 2);
		int i = 0;
		int64_t to_compare;
		while (i < number_of_keys) {
			memcpy(&to_compare, search->frame + header_page_size + i * 16, 8);
			if (key >= to_compare) i++;
			else break;
		}
		memcpy(&search_offset, search->frame + 120 + 16 * i, 8);
		ask_buffer_manager(search_offset);
	}
	return search;
}

char* find(int table_id, int64_t key) {

	int find = false;

	buffer_structure* leaf = find_leaf(key);

	if (leaf == NULL) return NULL;
	//get #keys in found leaf page [12-16]
	int number_of_keys;
	memcpy(&number_of_keys, leaf->frame + 12, 4);

	//linear seach get i
	int i = 0;
	int64_t to_compare;
	while (i < number_of_keys) {
		//coparison at most #current_keys
		//get key to compare [128-136] [256-264]
		memcpy(&to_compare, leaf->frame + header_page_size + i * 128, 8);

		//if (key > to_compare) i++; //if key is bigger, look for next one.
		//else
		if (key == to_compare) {
			find = true;
			break;
		}
		i++;
	}

	//if found, use i to extract value.
	if (find) {
		char* value_ = (char*)malloc(sizeof(char) * 120);
		memcpy(value_, leaf->frame + header_page_size + 128 * i + 8, 120);
		return value_;
	}
	else {
		return NULL;
	}
}

buffer_structure* make_leaf_page() {

	buffer_structure* free_page_list = ask_buffer_manager(FREE_PAGE_LIST_OFFSET);

	int64_t new_leaf_offset;
	memcpy(&new_leaf_offset, free_page_list->frame, 8);

	buffer_structure* new_leaf = ask_buffer_manager(new_leaf_offset);

	//filling first 16 bytes of new_leaf->frame
	memcpy(new_leaf->frame, &bit64_0, 8);
	int leaf_ = 1;
	memcpy(new_leaf->frame + 8, &leaf_, 4);
	int number_of_key = 0;
	memcpy(new_leaf->frame + 12, &number_of_key, 4);

	make_free_page();

	return new_leaf; // this will be used as new_leaf_page offset
}

buffer_structure* make_internal_page() {
	//filling number of keys and is leaf
	buffer_structure* free_page_list = ask_buffer_manager(FREE_PAGE_LIST_OFFSET);

	int64_t new_leaf_offset;
	memcpy(&new_leaf_offset, free_page_list->frame, 8);

	buffer_structure* new_internal = ask_buffer_manager(new_leaf_offset);

	//filling first 16 bytes of new_internal->frame
	memcpy(new_internal->frame, &bit64_0, 8);
	int leaf_ = 0;
	memcpy(new_internal->frame + 8, &leaf_, 4);
	int number_of_key = 0;
	memcpy(new_internal->frame + 12, &number_of_key, 4);

	make_free_page();

	return new_internal;
}

void make_free_page() {

	buffer_structure* header_page = ask_buffer_manager(HEADER_PAGE_OFFSET);
	buffer_structure* free_page_list = ask_buffer_manager(FREE_PAGE_LIST_OFFSET);

	int64_t number_of_pages;
	memcpy(&number_of_pages, header_page->frame + 16, 8);

	int64_t new_free_page_offset = number_of_pages*page_size;
	buffer_structure* new_free_page = ask_buffer_manager(new_free_page_offset);

	memcpy(free_page_list->frame, &new_free_page_offset, 8);
	memcpy(new_free_page->frame, &bit64_0, 8);

	number_of_pages++;
	memcpy(header_page->frame + 16, &number_of_pages, 8);

	return;
}

void make_header_page() {

	int64_t number_of_page = 2;

	buffer_structure* new_header = ask_buffer_manager(HEADER_PAGE_OFFSET);
	buffer_structure* new_free_page_list = ask_buffer_manager(FREE_PAGE_LIST_OFFSET);
	memcpy(new_header->frame, &new_free_page_list->page_offset, 8);
	memcpy(new_header->frame + 8, &bit64_0, 8);
	memcpy(new_free_page_list->frame, &bit64_0, 8);
	memcpy(new_header->frame + 16, &number_of_page, 8);

	make_free_page();
}

int open_db(const char* pathname) {

	//raise up header and then get root ( initially 0 )
	
	FILE* fp;
	if ((fp = fopen(pathname, "rb+")) == 0) { //not exists. then create
		
		fp = fopen(pathname, "wb+");
		fd = fileno(fp);
		
		make_header_page(); //header_page and free_page_list is raised

	}
	else 
		fd = fileno(fp);

	buffer_structure* header = ask_buffer_manager(HEADER_PAGE_OFFSET);
	memcpy(&root, header->frame + 8, 8);

	int64_t num_of_pages; memcpy(&num_of_pages, header->frame + 16, 8);
	printf("number of pages :%" PRId64"\n", num_of_pages);
	int64_t free_page_offset; memcpy(&free_page_offset, header->frame, 8);
	printf("free_page : p.%" PRId64"\n", free_page_offset / 4096);
	int64_t root_page_offset; memcpy(&root_page_offset, header->frame + 8, 8);
	printf("root_page offset : %" PRId64"\n", root_page_offset);

	return 0;
}

int main(int argc, const char* argv[]) {

	char command;
	int64_t insert_key;
	int opened = false;
	printf("> ");
	//make buffer
	if (argc == 1) {
		fprintf(stderr, "command line first argument error.\n");
		fprintf(stderr, "Usage : execute_command <buffer_limit>\n");
		exit(EXIT_FAILURE);
	}
	init_db(atoi(argv[1]));
	//make buffer end
	while (scanf("%c", &command) != EOF) {
		switch (command) {
		case 'i':
			scanf("%" PRId64, &insert_key);
			char value_[120]; scanf("%s", value_);
			if (opened)
				insert(get_table_id(), insert_key, value_);
			display();
			break;
		case 'd':
			if (opened)
				display();
			break;
		case 'r':
			scanf("%" PRId64, &insert_key);
			if (opened)
				delete_(get_table_id(),insert_key);
			display();
			break;
		case 'o':
			printf("opening ");
			if (fd != 0) close_db(table_count);
			char absolute_path[200]; getcwd(absolute_path, 199);
			char path[50];	scanf("%s", path);
			printf("%s/%s\n", absolute_path, path);
			open_db(path); //설정 된후 계속 바뀔 것이다.
			opened = true;
			printf("table id : %d", get_table_id());
			break;
		case 'f':
			scanf("%" PRId64, &insert_key);
			if (opened) {
				char* _value = find(insert_key);
				if (_value)
					printf("%s\n", _value);
				else
					printf("there is no matching record. sorry\n");
				free(_value);
			}
			break;
		case 'q':
			//모든 get_table_id에 대해서 close_db가 이루어 져야하는데?
			close_db(get_table_id());
			exit(EXIT_SUCCESS);
			break;
		default:
			break;
		}
		if (!opened)
			printf("operation cannot permitted. \"OPEN_DB\" should be preceded \n");
		while (getchar() != (int)'\n');
		printf("> ");
	}
	return 0;
}