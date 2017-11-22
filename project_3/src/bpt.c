#include "bpt.h"

int64_t root;//루트는 계속 가지고 있으면서 갱신해주자.
int fd; //initially 0
char char_null[120];
int64_t bit64_0;
int table_count=1;
buffer_manager* BUFFER_MANAGER;


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
	//printf("making new block\n");
	buffer_structure* new_buffer = (buffer_structure*)malloc(sizeof(buffer_structure));
	new_buffer->frame = (char*)malloc(page_size);
	pread(fd, new_buffer->frame, 4096, offset);// pread only here
	new_buffer->next = NULL;
	new_buffer->prev = NULL;
	new_buffer->table_id = table_count;
	new_buffer->is_dirty = 0;
	new_buffer->pin_count = 0;
	new_buffer->page_offset = offset;
	return new_buffer;
}

//normal definitions

void close_table(int table_id){
	return close_db(table_id);
}

int open_table(const char* path){
	return open_db(path);
}

void shutdown_db(){
	close_db(table_count);
}

void display() {
	printf("------------------------display------------------------\n");
	if (root == 0) {
		printf("empty tree.\n");
		return;
	}

	buffer_structure* header = ask_buffer_manager(HEADER_PAGE_OFFSET);
	int64_t numpages; memcpy(&numpages, header->frame + 16, 8);

	ENTRY *queue = (ENTRY*)calloc(numpages, sizeof(struct entry));

	int64_t queue_front = 0;
	int64_t queue_end = -1;
	ENTRY root_;
	root_.page = ask_buffer_manager(root);
	root_.depth = 0;
	queue[++queue_end] = root_; //push root

	int depth_by_now = -1;
	while (queue_front <= queue_end) {
		//get front and then pop
		ENTRY pos = queue[queue_front];
		queue_front++;
		if (depth_by_now != pos.depth) {
			if (depth_by_now != -1) printf("|\n");
			depth_by_now = pos.depth;
		}

		int numChild; memcpy(&numChild, pos.page->frame + 12, 4);
		//print key
		int key_record_size_ = test_leaf(pos.page) ? 128 : 16;

		printf("(p%" PRId64")", pos.page->page_offset / 4096);
		printf("| ");

		int i;
		int64_t temp;
		for (i = 0; i<numChild; i++) {
			memcpy(&temp, pos.page->frame + header_page_size + key_record_size_*i, 8);
			//printf("%" PRId64" ", temp);
		}

		//enqueue others
		//not for leaf
		if (test_leaf(pos.page)) {
			continue;
		}

		//for internal
		for (i = 0; i<numChild + 1; i++) {
			++queue_end;
			memcpy(&temp, pos.page->frame + header_page_size - 8 + 16 * i, 8);
			queue[queue_end].page = ask_buffer_manager(temp);
			queue[queue_end].depth = pos.depth + 1;
		}
	}

	printf("|\n");
	free(queue);
	return;
}


int get_table_id() {
	return table_count;
}


buffer_structure* ask_buffer_manager(int64_t offset) {
	//printf("-------------asking for page %" PRId64" --------\n",offset/4096);
	buffer_structure* search = BUFFER_MANAGER->MRU;
	if (search == NULL) {
		//printf("MRU NULL -> first ask_buffer\n");
		//MRU NULL
		buffer_structure* first_buffer_block = new_buffer_block(offset);
		BUFFER_MANAGER->MRU = first_buffer_block;
		BUFFER_MANAGER->LRU = first_buffer_block;
		first_buffer_block->next = NULL;
		first_buffer_block->prev = NULL;
		//show_me_buffer();
		return first_buffer_block;
	}

	//MRU NOT NULL
	//at least one element in the list
	//printf("MRU NOT NULL -> searching page that has offset\n");
	for (; search != NULL; search = search->next) {

		if (!memcmp(&search->page_offset, &offset, 8)) {
			//printf("cache hit\n");
			//cache hit search.offset == offset
			//chacge location in list
			//remove
			if (BUFFER_MANAGER->MRU == search){
				//show_me_buffer();
				return search;
			}
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
			//show_me_buffer();

			return search;
		}
	}
	//cache not hit 
	//MRU NOT NULL
	//at least one element in the list
	//printf("cache not hit\n");
	if (search == NULL) {
		//search has no meaning from now on
		buffer_structure* new_block = new_buffer_block(offset);

		new_block->next = BUFFER_MANAGER->MRU;
		new_block->prev = new_block->next->prev; //R VALUE is NULL
		new_block->next->prev = new_block;
		BUFFER_MANAGER->MRU = new_block;

		//remove into list
		if (BUFFER_MANAGER->num_current_blocks == BUFFER_MANAGER->max_blocks) {
			//printf("buffer full : finding victom\n");
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
			//if (victim->is_dirty == true) {
				update(victim); fsync(fd);
			//}
		}
		else {
			//buffer not full
			//printf("buffer not full\n");
			BUFFER_MANAGER->num_current_blocks++;
		}
		//show_me_buffer();
		return new_block;
	}

}


void close_db(int table_id_) {
	//printf("current buffer_fill : %d\n",BUFFER_MANAGER->num_current_blocks);
	buffer_structure* ptr;
	for (ptr = BUFFER_MANAGER->MRU; ptr != NULL; ptr = ptr->next) {
		buffer_structure* current_block_ptr = ptr;
		if (ptr->table_id == table_id_) {
			//printf("close_db : writing buffer to disk\n");
			//unlink
			if (current_block_ptr->prev == NULL)
				BUFFER_MANAGER->MRU = current_block_ptr->next;
			else
				current_block_ptr->prev->next = current_block_ptr->next;

			if (current_block_ptr->next == NULL)
				BUFFER_MANAGER->LRU = current_block_ptr->prev;
			else
				current_block_ptr->next->prev = current_block_ptr->prev;

			//update and then free heap
			update(current_block_ptr);
			free(current_block_ptr);
			BUFFER_MANAGER->num_current_blocks--;
		}
	}

	fsync(fd);
	return;

}
void init_db(int buffer_size) {

	//initialize BUFFER_MANAGER
	//printf("making buffer_manager\n");
	BUFFER_MANAGER = new_buffer_manager(buffer_size);
	BUFFER_MANAGER->max_blocks = buffer_size;
	BUFFER_MANAGER->MRU = NULL;
	BUFFER_MANAGER->LRU = NULL;

}

void update(buffer_structure* block) {
	
	pwrite(fd, block->frame, 4096, block->page_offset); fsync(fd);

}


void return_to_free_page(buffer_structure* page) {
	//insert into the stack-implemented free_pages.
	// (stack) 
	buffer_structure* free_page = ask_buffer_manager(FREE_PAGE_LIST_OFFSET);
	int64_t temp; memcpy(&temp, free_page->frame, 8);
	memcpy(page->frame, &temp, 8);
	memcpy(free_page->frame, &page->page_offset, 8);
	return;
}

int adjust_root() {
	//printf("adjusting root\n");
	//after remove key from page, numKeys could be 0;

	buffer_structure* current_root = ask_buffer_manager(root);
	int numKeys; memcpy(&numKeys, current_root->frame + 12, 4);

	if (numKeys) {
		return 0;
	}
	else {

		if (test_leaf(current_root)) {
			//destroy tree
			return_to_free_page(current_root);
			
			buffer_structure* header = ask_buffer_manager(HEADER_PAGE_OFFSET);
			memcpy(header->frame + 8, &bit64_0, 8);
			
			root = 0;
		}
		else {
			//root is internal
			int64_t new_root_offset; memcpy(&new_root_offset, current_root->frame + header_page_size - 8, 8);
			buffer_structure* new_root = ask_buffer_manager(new_root_offset);
			buffer_structure* header = ask_buffer_manager(HEADER_PAGE_OFFSET);
			return_to_free_page(current_root);
			
			root = new_root->page_offset;
			memcpy(header->frame + 8, &root, 8);
			memcpy(new_root->frame, &bit64_0, 8);
		}
	}
	return 0;
}


int coalesce_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page, int neighbor_index, int64_t prime_key) {
	//printf("coalescing!\n");

	int numKeys_neighbor;
	int numKeys_page;

	if (neighbor_index == -1) {
		buffer_structure* temp = neighbor_page;
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
			
			//set right_side(page)'s child parent to left_side(neighbor_page)
			
			int64_t child_offset; 
			memcpy(&child_offset, page->frame + header_page_size - 8 + 16 * i, 8);

			buffer_structure* child = ask_buffer_manager(child_offset);
			memcpy(child->frame,&neighbor_page->page_offset, 8);

		}
		memcpy(neighbor_page->frame + header_page_size + 16 * numKeys_neighbor
			, &prime_key, 8);
		memcpy(neighbor_page->frame + header_page_size + 16 * numKeys_neighbor + 8
			, page->frame + header_page_size - 8
			, 16 * numKeys_page + 8);
		numKeys_neighbor += 1; //important
	}
	else {
		//leaf. so no child.
		memcpy(neighbor_page->frame + header_page_size - 8
			, page->frame + header_page_size - 8, 8);
		memcpy(neighbor_page->frame+header_page_size+key_record_size*numKeys_neighbor
			,page->frame+header_page_size
			, key_record_size*numKeys_page);

	}

	numKeys_neighbor += numKeys_page;
	delete_entry(parent, prime_key); //prime_key down.
	memcpy(neighbor_page->frame + 12, &numKeys_neighbor, 4);
	return_to_free_page(page);
	return 0;
}


int page_key_record_size(buffer_structure* page) {
	if (test_leaf(page))
		return 128;
	else
		return 16;
}


int redistribute_pages(buffer_structure* parent, buffer_structure* page, buffer_structure* neighbor_page
	, int neighbor_index, int prime_key_index, int64_t prime_key) {
	//printf("redistributing!\n");
	//int64_t parent; pread(fd, &parent, 8, page);
	int64_t new_key;

	int numKeys_parent;  memcpy(&numKeys_parent, parent->frame + 12, 4);
	int numKeys_neighbor; memcpy(&numKeys_neighbor, neighbor_page->frame + 12, 4);
	int numKeys_page; memcpy(&numKeys_page, page->frame + 12, 4);

	// neighbor   <->    original
	if (neighbor_index != -1) {
		//shifting byte
		if (!test_leaf(page))
			//internal page
			//shift by 16
			memmove(page->frame + header_page_size + 8, page->frame + header_page_size - 8, 16 * get_numKeys_of_page(page) + 8);
		else
			//leaf page
			//shift by 128
			memmove(page->frame + header_page_size + key_record_size, page->frame + header_page_size, key_record_size*get_numKeys_of_page(page));
		//filling byte
		if (!test_leaf(page)) {
			//internal page
			//page key[0] pointer[0]
			memcpy(page->frame + header_page_size, &prime_key, 8);
			memcpy(page->frame + header_page_size - 8, neighbor_page->frame + header_page_size * 16 * numKeys_neighbor - 8, 8);
			//parent key[prime_key_index]
			memcpy(parent->frame + header_page_size + 16 * prime_key_index, neighbor_page->frame + header_page_size + 16 * numKeys_neighbor, 8);
			//renew page pointer[0]'s parent from neighbor to page
			int64_t child_offset; memcpy(&child_offset, page->frame + header_page_size - 8, 8);
			buffer_structure* child = ask_buffer_manager(child_offset);
			memcpy(child->frame, &page->page_offset, 8);

		}
		else  //leaf
			memmove(page->frame + header_page_size,
				neighbor_page->frame + header_page_size + 128 * (numKeys_neighbor - 1), 128);

	}

	// original   <->   neighbor

	else {
		
		//internal page
		if (!test_leaf(page)) {
			memcpy(page->frame + header_page_size + 16 * numKeys_page, &prime_key, 8);
			memcpy(parent->frame + header_page_size + 16 * prime_key_index, neighbor_page->frame + header_page_size + 16, 8);
			memcpy(page->frame + header_page_size + 16 * numKeys_page + 8, neighbor_page->frame + header_page_size - 8, 8);
			int64_t child_offset; memcpy(&child_offset, neighbor_page->frame + header_page_size - 8, 8);
			buffer_structure* child = ask_buffer_manager(child_offset);
			memcpy(child->frame,&page->page_offset, 8);
		}
		else {
			memcpy(page->frame + header_page_size + key_record_size*numKeys_page
				, neighbor_page->frame + header_page_size, 128);
			memcpy(parent->frame + header_page_size + prime_key_index * 16,
				neighbor_page->frame + header_page_size + 128, 8);
		}

		if (!test_leaf(page))
			//internal page
			//shift by 16
			memmove(neighbor_page->frame + header_page_size - 8
				, neighbor_page->frame + header_page_size + 8
				, 16 * (numKeys_neighbor - 1) + 8);
		else
			//leaf page
			//shift by 128
			memmove(neighbor_page->frame + header_page_size,
				neighbor_page->frame + header_page_size + key_record_size
				, 128 * (numKeys_neighbor - 1));

	}

	numKeys_neighbor--; memcpy(neighbor_page->frame + 12, &numKeys_neighbor, 4);
	numKeys_page++; memcpy(page->frame+12, &numKeys_page, 4);

}

int get_neighbor_index(buffer_structure* parent,buffer_structure* page) {
	
	//page의 neighbor_index를 구할 것이다.

	int i = 0;
	int64_t to_compare;

	while (i < get_numKeys_of_page(parent)) {
		//scan [120-128] [136-144] ...
		//offset 비교를 한다.
		memcpy(&to_compare, parent->frame + header_page_size - 8 + 16 * i, 8);

		if (to_compare == page->page_offset) 
			break;
		else
			i++;

	}

	return i - 1;

}

int delete_entry(buffer_structure* page, int64_t key) {

	if (remove_entry_from_page(page, key) == -1) //not found
		return -1;

	if (page->page_offset == root) //deletion at root -> maybe root has to be disappeared.
		return adjust_root();

	int min_keys = test_leaf(page) ? cut(get_order_of_current_page(page) - 1) : cut(get_order_of_current_page(page)) - 1;

	if (get_numKeys_of_page(page) >= min_keys) {
		//printf("delete finished (enough numKeys)\n");
		return 0;
	}

	//get parent of page
	int64_t parent_offset; memcpy(&parent_offset, page->frame, 8);
	buffer_structure* parent = ask_buffer_manager(parent_offset);
	
	//get neighbor_index & prime_key & numKeys
	int neighbor_index = get_neighbor_index(parent, page); // [0-numKeys] not [-1 to numKeys-1];
	int prime_key_index = neighbor_index == -1 ? 0 : neighbor_index;// [0-(numKeys -1)]
	int temp = (neighbor_index == -1) ? 1 : neighbor_index;

	
	int64_t neighbor_offset; memcpy(&neighbor_offset, parent->frame + header_page_size - 8 + 16 * temp, 8);
	buffer_structure* neighbor = ask_buffer_manager(neighbor_offset);
	
	//page와 neighbor_page 사이의 key값을 구한다.
	int64_t prime_key; memcpy(&prime_key, parent->frame + header_page_size + 16 * prime_key_index,8);
	
	// 31 for leaf and 248 for internal
	int capacity = get_order_of_current_page(page) - 1;

	if (get_numKeys_of_page(neighbor) + get_numKeys_of_page(page) <= capacity)
		return coalesce_pages(parent, page, neighbor, neighbor_index, prime_key);
	else
		return redistribute_pages(parent, page, neighbor, neighbor_index, prime_key_index, prime_key);
	return 0;
}

int delete(int table_id, int64_t key) {
	
	char* key_record = find(get_table_id(), key);
	buffer_structure* leaf = find_leaf(key);

	if (key_record != 0 && leaf !=NULL) {
		delete_entry(leaf, key);
		free(key_record);
	}
	else {
		//printf("there is no record that matches with the key (%" PRId64").\n",key);
	}
	return 0;
}

int remove_entry_from_page(buffer_structure* page, int64_t key) {
	
	int64_t to_compare;
	int64_t keyPointer_or_keyRecord_size = test_leaf(page) ? key_record_size : 16;

	int numKeys = get_numKeys_of_page(page);

	int i = 0;
	while (i < numKeys) {
		
		memcpy(&to_compare, page->frame + header_page_size + i*keyPointer_or_keyRecord_size, 8);

		if (to_compare == key) 
			break;
		else 
			i++;

	}
	if (to_compare != key) {
		//printf("not found sorry. T^T\n");
		return -1;
	}

	//key found

	memmove(page->frame + header_page_size + i*keyPointer_or_keyRecord_size
		, page->frame + header_page_size + (i + 1)*keyPointer_or_keyRecord_size
		, (numKeys - 1 - i)*keyPointer_or_keyRecord_size);

	numKeys--;
	memcpy(page->frame + 12, &numKeys, 4);

	//maybe we can set the other pointers and keys to null for tidiness

	return 0;
}

void insert_into_node_after_splitting(buffer_structure* left, int left_index, int64_t new_key,buffer_structure* right_child) {
	//printf("inserting into new internal page.\n");
	int split;
	//get parent from right child.
	int order_ = get_order_of_current_page(left);
	//make new internal page.
	buffer_structure* right = make_internal_page();
	//printf("new internal_page created : p.%" PRId64"\n", right->page_offset/4096);
	char* temp = (char*)malloc(order_ * 16);

	//copy to temp
	memcpy(temp, left->frame + header_page_size, 16 * left_index);
	memcpy(temp + left_index * 16, &new_key, 8);
	memcpy(temp + left_index * 16 + 8, &right_child->page_offset, 8);
	memcpy(temp + 16 * (left_index + 1)
		, left->frame + header_page_size + 16 * left_index
		, 16 * (order_ - 1 - left_index));

	split = cut(order_);
	//printf("order_: %d\n",order_);
	//printf("split : %d\n",split);

	//renew numKeys
	int numKeys_left = split;
	memcpy(left->frame + 12, &numKeys_left, 4);

	int numKeys_right = order_ - split - 1; 
	memcpy(right->frame + 12, &numKeys_right, 4);
	
	memcpy(right->frame + header_page_size - 8
		, temp + split * 16 + 8
		, 16 * (order_ - split - 1) + 8);

	//extract new_key
	int64_t prime_key;
	memcpy(&prime_key
		, left->frame + header_page_size + 16 * split, 8);

	//new_internal_page에 있는 child 들의 부모를 new_internal_page로 바꿔주어야 한다.
	/*	
	int k;
	printf("numKeys_right : %d\n",numKeys_right);
	for (k=0;k<numKeys_right*2+1;k++){
		int64_t hello; memcpy(&hello,right->frame+header_page_size-8+8*k,8);
		printf("%" PRId64"\n",hello);

	}
	*/

	int i;
	for (i = 0; i < numKeys_right + 1; i++) {

		int64_t child_offset; 

		memcpy(&child_offset
			, right->frame + header_page_size - 8 + 16 * i, 8);

		buffer_structure* child = ask_buffer_manager(child_offset);

		memcpy(child->frame
			, &right->page_offset
			, 8);

	}
	
	//set right's parent
	int64_t parent_of_left; memcpy(&parent_of_left, left->frame, 8);
	memcpy(right->frame, &parent_of_left, 8);

	free(temp);
	//display();
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
	memcpy(&number_of_keys, new_root->frame + 12, 4); //now 0
	number_of_keys++; 
	memcpy(new_root->frame + 12, &number_of_keys, 4); //now 1

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
		insert_into_node_after_splitting(parent, left_index, new_key,right); 
	
	// #key already 248, become 249 then splitted #order = 249
}


void insert_into_leaf_after_splitting(buffer_structure* leaf, int64_t key, char* value) {
	//printf("insert into leaf after splitting\n");
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


void insert_into_leaf(buffer_structure* leaf, int64_t key, char* value) {
	//printf("pushing into leaf : p.%" PRId64 "\n", leaf->page_offset/4096);
	
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
	if (find(table_id,key) != 0) {
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
	if (is_leaf) 
		return true;
	else 
		return false;
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
		//printf("empty tree\n");
		return NULL; //empty tree : return 0 to find function
	}

	//ask for root buffer_structure
	int64_t search_offset = root;
	buffer_structure* search = ask_buffer_manager(search_offset);
	while (true) {//linear search

		if (test_leaf(search)) {
			//printf("leaf found\n");
			break;
		}

		//not leaf
		//printf("enter internal page\n");
		int number_of_keys; memcpy(&number_of_keys, search->frame + 12, 4);

		int i = 0;
		int64_t to_compare;

		while (i < number_of_keys) {

			memcpy(&to_compare, search->frame + header_page_size + i * 16, 8);
			
			if (key >= to_compare) 
				i++;
			else 
				break;

		}
		memcpy(&search_offset
			, search->frame + header_page_size - 8 + 16 * i, 8);
		//printf("search_offset : %" PRId64"\n",search_offset);
		search = ask_buffer_manager(search_offset);
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
	int64_t new_internal_offset;
	memcpy(&new_internal_offset, free_page_list->frame, 8);
	//printf("new_internal offset : %" PRId64"\n",new_internal_offset);

	buffer_structure* new_internal = ask_buffer_manager(new_internal_offset);

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

void show_me_buffer(){
	buffer_structure* trace = BUFFER_MANAGER->MRU;
	while(trace!=NULL){

		fprintf(stderr,"%3" PRId64" ",trace->page_offset/4096);
		trace = trace -> next;
	}
	fprintf(stderr,"\n");
	return;
}


int open_db(const char* pathname) {

	//raise up header and then get root ( initially 0 )
	table_count++;
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
	/*
	int64_t num_of_pages; memcpy(&num_of_pages, header->frame + 16, 8);
	printf("number of pages :%" PRId64"\n", num_of_pages);
	int64_t free_page_offset; memcpy(&free_page_offset, header->frame, 8);
	printf("free_page : p.%" PRId64"\n", free_page_offset / 4096);
	int64_t root_page_offset; memcpy(&root_page_offset, header->frame + 8, 8);
	printf("root_page offset : %" PRId64"\n", root_page_offset);
	*/
	return table_count;
}