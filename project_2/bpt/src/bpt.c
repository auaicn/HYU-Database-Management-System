#include "bpt.h"

// GLOBALS.

int64_t root = 0;
int fd = -1;
char char_null[120];
int64_t bit64_0 = 0;

void display(){
	if(root==0){
		printf("empty tree.\n");
		return;
	}
	int64_t numpages; pread(fd,&numpages,8,16);
	ENTRY *queue = (ENTRY*) calloc(numpages,sizeof(struct entry));
	
	int64_t queue_front = 0;
	int64_t queue_end = -1;
	ENTRY root_;
	root_.offset = root;
	root_.depth = 0;
	queue[++queue_end] = root_; //push root

	int depth_by_now = -1;
	while(queue_front<=queue_end){
		//get front and then pop
		ENTRY pos = queue[queue_front++];
		if(depth_by_now!=pos.depth){
			if(depth_by_now!=-1) printf("|\n");
			depth_by_now = pos.depth;
		}

		int numChild; pread(fd,&numChild,4,12+pos.offset);
		//print key
		int key_record_size_ = test_leaf(pos.offset)?128:16;
		
		//printf("(p.%" PRId64")",pos.offset/4096);
		int i;
		printf("| ");
		for (i=0;i<numChild;i++){
			printf("%" PRId64" ",value_read(pos.offset+header_page_size+key_record_size_*i,8));
		}

		//enqueue others
		//not for leaf
		if(test_leaf(pos.offset)) continue;
		//for internal
		for (i=0;i<numChild+1;i++){
			++queue_end;
			queue[queue_end].offset = value_read(pos.offset+header_page_size - 8 + 16 * i,8 );
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
	swrite(fd, &temp, 8, offset);
	swrite(fd, &offset, 8, 4096);
	return;
}

void adjust_root() {
	//printf("adjusting root\n");
	int num; pread(fd, &num, 4, root + 12); 
	if (num) return; //there's key in root -> no need to adjust.
	else{
		if(test_leaf(root)){ //make tree to be empty.
			
			return_to_free_page(root);
			swrite(fd,&bit64_0,8,8);
			//no need to set parent of root cause it's null
			root = 0;
		}
		else{
			int64_t new_root; pread(fd, &new_root, 8, root + header_page_size - 8); //get first child.

			return_to_free_page(root); //free root and then record ne root.
			swrite(fd, &new_root, 8, 8);
			swrite(fd,&bit64_0,8,new_root); //new root has no parent.
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
			swrite(fd, &neighbor_page, 8, child);
		}
		swrite(fd, &prime_key, 8, neighbor_page + header_page_size + 16 * (numKeys_neighbor));
		numKeys_neighbor++;
		shift_byte(page + header_page_size - 8, neighbor_page + header_page_size - 8 + 16 * numKeys_neighbor, 16 * numKeys_neighbor + 8);
		numKeys_neighbor += 1; //important
	}
	else {
		//leaf 
		//neighbor - original
		shift_byte(page + header_page_size, neighbor_page + header_page_size + 128 * numKeys_neighbor, 128 * numKeys_page);
		shift_byte(page+header_page_size-8,neighbor_page+header_page_size-8,8);

	}
	numKeys_neighbor += numKeys_page;
	delete_entry(parent, prime_key); //prime_key down.
	swrite(fd, &numKeys_neighbor, 4, neighbor_page + 12); //renew merged 
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
			swrite(fd, &temp_key, 8, page + header_page_size);
			//just copied and then not recorded into parent
			int64_t temp_offset;
			pread(fd, &temp_offset, 8, neighbor_page + header_page_size + 16 * (numKeys_neighbor - 1) + 8);
			swrite(fd, &temp_key, 8, page + header_page_size - 8);

		}
		else { //leaf
			shift_byte(neighbor_index + header_page_size + 128 * (numKeys_neighbor - 1), page + header_page_size, 128);
		}
		
		pread(fd, &new_key, 8, page + header_page_size);
		swrite(fd, &new_key, 8, parent + header_page_size + prime_key_index*page_key_record_size(page));

	}

	// neighbor is at right

	else {
		
		//filling byte
			//internal page
		if (!test_leaf(page)) {
			int64_t temp_key;
			pread(fd, &temp_key, 8, neighbor_page + header_page_size);
			swrite(fd, &temp_key, 8, page + header_page_size + 16 * numKeys_page);
			int64_t temp_offset;
			pread(fd, &temp_offset, 8, neighbor_page + header_page_size - 8);
			swrite(fd, &temp_offset, 8, page + header_page_size + 16 * numKeys_page + 8);
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
		swrite(fd, &new_key, 8, parent + header_page_size + prime_key_index*page_key_record_size(parent));
	

	}

	numKeys_neighbor--; swrite(fd, &numKeys_neighbor, 4, 12 + neighbor_page);
	numKeys_page++; swrite(fd, &numKeys_page, 4, 12 + page);

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

void delete_entry(int64_t page, int64_t key){
		
	remove_entry_from_page(key, page);

	if (page == root) //deletion at root -> maybe root has to be disappeared.
		return adjust_root();

	int min_keys = get_order_of_current_page(page)/2;

	if (get_numKeys_of_page(page) >= min_keys){
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
	int capacity = get_order_of_current_page(page)-1;
	// 31 for leaf and 248 for internal

	if (get_numKeys_of_page(neighbor) + get_numKeys_of_page(page) <= capacity)
		return coalesce_pages(page, neighbor, neighbor_index, prime_key);
	else
		return redistribute_pages(page, neighbor, neighbor_index, prime_key_index, prime_key);

}

int delete(int64_t key) {
	char* key_record = find(key);
	int64_t key_leaf = find_leaf(key);
	if (key_record != 0 && key_leaf != 0) {
		delete_entry(key_leaf,key);
		free(key_record);
	}else{
		//printf("there is no record that matches with the key (%" PRId64").\n",key);
	}
	return 0;
}

void remove_entry_from_page(int64_t key,int64_t offset) {
	int i = 0;
	int64_t key_pointer_or_record_size = test_leaf(offset) ? key_record_size : 16;
	int numKeys = get_numKeys_of_page(offset);
	
	int64_t to_compare;
	while (i < numKeys) {
		pread(fd, &to_compare, 8, offset + header_page_size + i*key_pointer_or_record_size);
		if(to_compare==key) break;
		else i++;
	}
	if (to_compare != key) {
		//printf("not found sorry. T^T\n");
		return;
	}

	//key found
	shift_byte(offset + header_page_size + (i + 1)*key_pointer_or_record_size, offset + header_page_size + i*key_pointer_or_record_size, (numKeys - 1 - i)*key_pointer_or_record_size);
	
	numKeys--;
	swrite(fd, &numKeys, 4, offset + 12);

	//maybe we can set the other pointers and keys to null for tidiness

}

ssize_t swrite(int fd, const void* buf, int64_t size_, int64_t offset) {
	ssize_t n = pwrite(fd, buf, size_, offset); fsync(fd);
	return n;
}


void insert_into_node_after_splitting(int left_index, int64_t new_key, int64_t right) {
	//printf("inserting into new internal page.\n");
	int split;
	//get parent from right child.
	int64_t parent; pread(fd, &parent, 8, right);
	int order_ = get_order_of_current_page(parent);
	//make new internal page.
	int64_t new_internal_page_offset = make_internal_page();
	//printf("new internal_page created : p.%" PRId64"\n", new_internal_page_offset/4096);
	char* temp = (char*)malloc(order_ * 16);
	//16*left_index copy
	pread(fd, temp, left_index * 16, parent + header_page_size);
	//copy new_key and right
	memcpy(temp + left_index * 16, &new_key, 8);
	memcpy(temp + left_index * 16 + 8, &right, 8);
	//16*(order_-1-left_inex) copy
	pread(fd, temp, 16 * (order_ - 1 - left_index), parent + header_page_size + (left_index + 1) * 16);

	split = cut(order_);

	//renew number of keys [12-16]
	int num_left_key = split - 1;
	swrite(fd, &num_left_key, 4, parent + 12);
	int num_keys_of_new_page = order_ - (split - 1) - 1;
	swrite(fd, &num_keys_of_new_page, 4, new_internal_page_offset + 12);
	//write to new one [120- ]
	swrite(fd, temp + split * 16 - 8, 16 * (order_ - split) + 8, new_internal_page_offset + header_page_size - 8);
	//extract new_key
	int64_t prime_key; pread(fd, &prime_key, 8, parent + header_page_size + 16 * (split - 1));
	//new_internal_page에 있는 child 들의 부모를 new_internal_page로 바꿔주어야 한다.
	int i;
	for (i = 0; i < num_keys_of_new_page + 1; i++) {
		int64_t child_offset; pread(fd, &child_offset, 8, new_internal_page_offset + 120 + 16 * i);
		swrite(fd, &new_internal_page_offset, 8, child_offset);
	}
	//set new page's parent
	int64_t parent_of_parent; pread(fd, &parent_of_parent, 8, parent);
	swrite(fd, &parent_of_parent, 8, new_internal_page_offset);
	//free memory
	free(temp);

	insert_into_parent(parent, new_internal_page_offset, prime_key);
}

void insert_into_node(int left_index, int64_t parent, int64_t new_key, int64_t right) {
	//no overflow then just insert it.
	int numKeys; pread(fd, &numKeys, 4, parent + 12);
	int64_t start_offset = parent + header_page_size + 16 * left_index;
	shift_byte(start_offset, start_offset + 16, 16 * (numKeys - left_index));
	swrite(fd, &new_key, 8, start_offset);
	swrite(fd, &right, 8, start_offset + 8);
	numKeys++; swrite(fd, &numKeys, 4, parent + 12);
}


void insert_into_new_root(int64_t left, int64_t key, int64_t right) {
	int64_t new_root_offset = make_internal_page();
	//renew number of keys
	int number_of_keys; pread(fd, &number_of_keys, 4, new_root_offset + 12);
	number_of_keys++; swrite(fd, &number_of_keys, 4, new_root_offset + 12);
	//set parent offset to 0
	swrite(fd, &bit64_0, 8, new_root_offset);
	swrite(fd, &new_root_offset, 8, left);
	swrite(fd, &new_root_offset, 8, right);
	//set key, pointer 
	swrite(fd, &left, 8, new_root_offset + 120);
	swrite(fd, &key, 8, new_root_offset + 128);
	swrite(fd, &right, 8, new_root_offset + 136);
	//renew global root.
	root = new_root_offset;
	swrite(fd,&root,8,8);
	//printf("new root created. pageno is %" PRId64"\n", root/4096);
}


void insert_into_parent(int64_t left, int64_t right, int64_t new_key) {
	//from splitting. they both passes left and right child offset and new_key
	//offset of parent
	int64_t parent; pread(fd, &parent, 8, left);
	if (parent == 0){
		insert_into_new_root(left, new_key, right);
		return;
	}

	int left_index = 0;
	int64_t to_compare;
	while (left_index < get_numKeys_of_page(parent)) {
		pread(fd, &to_compare, 8, parent + header_page_size + left_index * 16); // 0 -> 8byte from header_page_size + track
		if (new_key > to_compare) left_index++;
		else break;
	}
	if (get_numKeys_of_page(parent) < get_order_of_current_page(parent) - 1)
		insert_into_node(left_index, parent, new_key, right);
	else insert_into_node_after_splitting(left_index, new_key, right); // #key already 248, become 249 then splitted #order = 249
}


void insert_into_leaf_after_splitting(int64_t leaf_offset, int64_t key, char* value) {
	int split;

	int64_t new_leaf = make_leaf_page();
	//printf("new leaf created : p.%" PRId64 "\n", new_leaf);

	shift_byte(leaf_offset, new_leaf, 8);
	int order_ = get_order_of_current_page(leaf_offset);
	char* temp = (char*)calloc(order_,128);
	
	int insertion_point = 0;
	int64_t to_compare;
	while (insertion_point < order_ - 1) {
		pread(fd, &to_compare, 8, leaf_offset + header_page_size
			+ insertion_point * 128);
		if (key > to_compare) insertion_point++;
		else break;
	}
	//copy to temp
	pread(fd, temp, insertion_point*key_record_size, leaf_offset + header_page_size);
	memcpy(temp + insertion_point*key_record_size, &key, 8);
	memcpy(temp + insertion_point*key_record_size + 8, value, 120);
	pread(fd, temp + key_record_size*(insertion_point + 1), (order_ - 1 - insertion_point)*key_record_size, leaf_offset + header_page_size + key_record_size*insertion_point);
	split = cut(order_);


	//temp to each node left & right
	swrite(fd, temp, key_record_size*split, leaf_offset + header_page_size);
	swrite(fd, temp + key_record_size*split, key_record_size*(order_ - split), new_leaf + header_page_size);
	
	//numKeys left & right
	swrite(fd, &split, 4, leaf_offset + 12);
	int num_right_key = order_ - split; swrite(fd, &num_right_key, 4, new_leaf + 12);
	
	//siblings control
	shift_byte(leaf_offset+header_page_size-8,new_leaf+header_page_size-8,8);
	swrite(fd, &new_leaf, 8, leaf_offset + 120);

	//new key is selected and send to the parent for high-level insert
	int64_t new_key = value_read(new_leaf+header_page_size,8);
	
	//free memory and call subroutine.
	free(temp);
	insert_into_parent(leaf_offset, new_leaf, new_key);
}


void shift_byte(int64_t start, int64_t end, int64_t size_) {
	char* temp = (char*)malloc(size_);
	pread(fd, temp, size_, start);
	swrite(fd, temp, size_, end);
	free(temp);
	return;
}

int64_t value_read(int64_t offset, int byte_) {
	int64_t temp; pread(fd, &temp, byte_, offset);
	return temp;
}

void insert_into_leaf(int64_t leaf_offset, int64_t key, char* value) {
	//printf("pushing into leaf : p.%" PRId64 "\n", leaf_offset/4096);
	int insertion_point;
	int num = get_numKeys_of_page(leaf_offset);
	insertion_point = 0;
	int64_t to_compare;
	while (insertion_point<num) {
		pread(fd, &to_compare, 8, insertion_point * 128 + header_page_size + leaf_offset);
		if (to_compare < key) insertion_point++;
		else break;
	}
	int64_t shift_start = leaf_offset + header_page_size + 128 * insertion_point;
	shift_byte(shift_start, shift_start + 128, 128 * (num - insertion_point));
	num++; swrite(fd, &num, 4, 12 + leaf_offset);
	swrite(fd, &key, 8, shift_start);
	swrite(fd, char_null, 120, shift_start + 8);
	swrite(fd, value, 120, shift_start + 8);

}

int insert(int64_t key, char* value) { //사실 root가 전역변수라서 return 해줘야 하는지도 모르겠다.
	if (find(key) != 0) {
		//printf("duplicate insert.\ntry again.\n");
		return 0;
	}
	if (root == 0) { //first insert
		int64_t root_offset = make_leaf_page();
		swrite(fd, &root_offset, 8, 8);
		//now we can access 8-16
		//한번은 직접 대입해주자
		//first key, first value
		swrite(fd, &key, 8, root_offset + header_page_size);
		swrite(fd, value, 120, root_offset + header_page_size + 8);

		//page header for root
		swrite(fd, &bit64_0, 8, root_offset); //parent of root is 0 (means there is no parent page)
											  //is leaf 8-12 is already written
		int numKeys; pread(fd, &numKeys, 4, root_offset + 12); //num of keys offset 12
		numKeys++; swrite(fd, &numKeys, 4, root_offset + 12);
		root = root_offset;
		return 0;
	}
	int64_t leaf = find_leaf(key);
	int max_numKeys = get_order_of_current_page(leaf)-1;
	if (get_numKeys_of_page(leaf) < max_numKeys) {
		//printf("%d < %d -> insert_into_leaf\n", get_numKeys_of_page(leaf), max_numKeys);
		insert_into_leaf(leaf, key, value);
		return 0;
	}
	insert_into_leaf_after_splitting(leaf, key, value); //why root not global variable?
	return 0;
}

int get_numKeys_of_page(int64_t page_offset) {
	int temp; pread(fd, &temp, 4, page_offset + 12);
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

int test_leaf(int64_t page_offset) {
	int is_leaf; pread(fd, &is_leaf, 4, page_offset + 8); //8바이트 이후로 4바이트 읽는다.
	if (is_leaf) return true;
	else return false;
	//leaf확인이 필요하면 나중에 또 부르거나 저장해서 쓰면 되겠지.
}


int get_order_of_current_page(int64_t page_offset) {
	//branch factor (order)
	//leaf : 32
	//internal : 249
	//invariant : numKeys is 1-less than order
	if (test_leaf(page_offset)) return 32;
	else return 249;
}

int64_t find_leaf(int64_t key) {
	int64_t track = root;
	if (root == 0) {//root offset initialized to 0
		//printf("Not Exists\n");
		return 0; //empty tree : return 0 to find function
	}
	while (true) {//linear search
		if (test_leaf(track)) {
			break;
		}
		//not leaf
		int number_of_keys; pread(fd, &number_of_keys, 4, track + 12);
		int i = 0;
		int64_t to_compare;
		while (i < number_of_keys) {
			pread(fd, &to_compare, 8, track + header_page_size + i * 16); // 0 -> 8byte from header_page_size + track
			if (key >= to_compare) i++;
			else break;
		}
		pread(fd, &track, 8, track + 120 + 16 * i);// renew offset 0 -> 8byte from 120 + track
	}
	return track;
}

char* find(int64_t key) {
	int find = false;
	int64_t leaf_offset = find_leaf(key); //found then linear search
	if (!leaf_offset) return 0;
	//get #keys in found leaf page [12-16]
	int number_of_keys; pread(fd, &number_of_keys, 4, leaf_offset + 12);
	//linear seach get i
	int i = 0;
	int64_t to_compare;
	while (i < number_of_keys) { //coparison at most #current_keys
								 //get key to compare [128-136] [256-264]
		pread(fd, &to_compare, 8, leaf_offset + header_page_size + i * 128);
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
		pread(fd, value_, 120, leaf_offset + header_page_size + 8 + 128 * i);// renew offset 0 -> 8byte from 120 + track
		return value_;
	}
	else {
		return 0;
	}
}

int64_t make_leaf_page() {
	//parent설정은 위에서 할 것이며
	//is leaf는 true;
	//number of keys 는 0으로 설정되어있다.
	int64_t new_leaf_offset; pread(fd, &new_leaf_offset, 8, 4096);
	
	/*
	// (stack) link again
	int64_t next; pread(fd,&next,8,new_leaf_offset);
	swrite(fd,&next,8,4096);
	*/
	//initialize leaf page
	swrite(fd, &bit64_0, 8, new_leaf_offset + 120); //right siblings : 0 initially
	int leaf_ = 1; swrite(fd, &leaf_, 4, 8 + new_leaf_offset);
	int number_of_key = 0; swrite(fd, &number_of_key, 4, 12 + new_leaf_offset);

	make_free_page();
	
	return new_leaf_offset; // this will be used as new_leaf_page offset
}

int64_t make_internal_page() {
	//filling number of keys and is leaf
	int64_t new_internal_offset; pread(fd, &new_internal_offset, 8, 4096);
	/*
	// (stack) link again
	int64_t next; pread(fd,&next,8,new_internal_offset);
	swrite(fd,&next,8,4096);
	*/
	//initialize internal page
	swrite(fd,&bit64_0,8,new_internal_offset);
	int leaf_ = 0; swrite(fd,&leaf_,4,8+new_internal_offset);
	int number_of_key = 0; swrite(fd,&number_of_key,4,12+new_internal_offset);
	
	make_free_page();
	
	return new_internal_offset;
}

int64_t make_free_page() {
	int64_t number_of_pages; pread(fd, &number_of_pages, 8, 16);
	
	int64_t new_free_page_offset = number_of_pages*page_size;
	//int64_t existing_page; pread(fd,&existing_page,8,4096);
	//what [4096-5104] has. it can be null.

	//there exists free page already. then do not make any.
	/*if(existing_page) 
		return;
	else {
	*/
		swrite(fd, &new_free_page_offset, 8, 4096);
		swrite(fd, &bit64_0, 8, new_free_page_offset); //next not existing
		/*
	}
	*/
	number_of_pages++; swrite(fd, &number_of_pages, 8, 16);
}

void make_header_page() {
	//open_file_fd fill 0-4096.
	//make page-list
	int64_t page_manager_offset = 4096;
	swrite(fd, &page_manager_offset, 8, 0);
	int64_t root_offset = 0;
	swrite(fd, &root_offset, 8, 8);
	swrite(fd,&bit64_0,8,4096);
	//write number of page including header page
	int64_t number_of_page = 2;
	swrite(fd, &number_of_page, 8, 16);
	make_free_page(); //then now number of page would be 3
}

int open_db(const char* pathname) {
	FILE* fp;
	if ((fp = fopen(pathname, "rb+")) == 0) { //not exists. then create
		fp = fopen(pathname, "wb+");
		fd = fileno(fp);
		make_header_page();
	}
	else fd = fileno(fp);
	pread(fd, &root, 8, 8);
	//int64_t num_of_pages; pread(fd, &num_of_pages, 8, 16);
	//printf("number of pages :%" PRId64"\n", num_of_pages);
	/*
	int64_t free_page_offset; pread(fd, &free_page_offset, 8, 0);
	printf("free_page : p.%" PRId64"\n", free_page_offset/4096);
	int64_t root_page_offset; pread(fd, &root_page_offset, 8, 8);
	printf("root_page offset : %" PRId64"\n", root_page_offset);
	*/
	return 0; //main has to know about open_db
}
