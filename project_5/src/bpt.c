#include "bpt.h"

/* global vars extern in bpt.h */
char* null120;
int current_fd;
int current_table_id;
int64_t current_root_offset;
int64_t zero;
int table_create_count;
int transaction_count;
int need_recovery;

buffer_manager* BUFFER_MANAGER;
table_manager* TABLE_MANAGER;
log_manager* LOG_MANAGER;

int display_current_transaction_log_on_memory()
{
	transactional_unit* looking_transaction = LOG_MANAGER->current_transaction;
	if (looking_transaction == NULL) {
		fprintf(stderr, "there's no current_transaction\n");
		return -1;
	}

	log* looking_log = looking_transaction->begin;

	char** table = (char**)malloc(sizeof(20*sizeof(char*)));
	for (int i = 0; i < 20; i++)
		table[i] = (char*)malloc(sizeof(char) * 100);

	table[0][8] = '\0';
	table[1][8] = '\0';
	table[2][4] = '\0';
	table[3][4] = '\0';
	table[4][4] = '\0';
	table[5][4] = '\0';
	table[6][4] = '\0';
	table[7][4] = '\0';
	while (looking_log) {
		int temp4; 
		int64_t temp8;
		temp8= getter_log_value_8byte(looking_log, LSN); 
		memcpy(table[0], &temp8, 8);
		temp8 = getter_log_value_8byte(looking_log, Prev_LSN);
		memcpy(table[1], &temp8, 8);
		temp4 = getter_log_value_4byte(looking_log, transaction_ID);
		memcpy(table[2], &temp4, 4);
		temp4 = getter_log_value_4byte(looking_log, Type);
		memcpy(table[2], &temp4, 4);
		temp4 = getter_log_value_4byte(looking_log, Table_ID);
		memcpy(table[2], &temp4, 4);
		temp4 = getter_log_value_4byte(looking_log, Page_Number);
		memcpy(table[2], &temp4, 4);
		temp4 = getter_log_value_4byte(looking_log, OFFSET);
		memcpy(table[2], &temp4, 4);
		temp4 = getter_log_value_4byte(looking_log, Data_Length);
		memcpy(table[2], &temp4, 4);
		for (int i = 0; i < 8; i++) {
			printf("%s\n", table[i]);
		}
		printf("\n");
		looking_log = looking_log->next;
	}

	for (int i = 0; i < 20; i++)
		free (table[i]);
	free (table);
	return 0;
}

void recovery()
{
	fprintf(stderr,"start recovery\n");
	fprintf(stderr, "now flushed_LSN is %ld\n", LOG_MANAGER->flushed_LSN);
	int started = false;

	while (true) {
		//open file if needed.
		//fetch needed page.
		//check if pageLSN is bigger than current looking flushedLSN.
		log* log = read_one_log();
		printf("read_one_log finished\n");
		if(log==NULL){
			fprintf(stderr,"EOF reached\n");
			break;
		}
		int type; memcpy(&type, log->frame + Type, 4);
		switch (type) {
		case BEGIN:
			fprintf(stderr,"BEGIN record\n");
			if (started) {
				fprintf(stderr, "double start\n");
				exit(EXIT_FAILURE);
			}
			started = true;
			break;
		case ABORT:
			fprintf(stderr, "ABORT record\n");
			if (!started) {
				fprintf(stderr, "not started but abort log");
				exit(EXIT_FAILURE);
			}
			started = false;
			break;
		case COMMIT:
			fprintf(stderr, "COMMIT record\n");
			if (!started) {
				fprintf(stderr, "not started but commit log");
				exit(EXIT_FAILURE);
			}
			started = false;
			break;
		case UPDATE:
			fprintf(stderr, "UPDATE record\n");
			if (started == false) {
				fprintf(stderr, "not started and update error\n");

				exit(EXIT_FAILURE);
			}
			// there is no table structure on memory.
			// we have to open these file.
			// if we cannot search the pathname from get_table_structure_by_name

			int table_id; memcpy(&table_id, log->frame + Table_ID, 4);
			char* pathname = (char*)malloc(sizeof(char) * 10);
			char* dataindex = (char*)malloc(sizeof(char) * 3);
			memcpy(pathname, "DATA", 4);

			if (table_id < 10) {
				pathname[4] = table_id + '0';
				pathname[5] = '\0';
			}
			else { //table_id == 10
				pathname[4] = '1';
				pathname[5] = '0';
				pathname[6] = '\0';
			}
			printf("calcuate pathname : %s\n", pathname);
			
			table_structure* table = get_table_structure_by_name(pathname);
			if (table == NULL) {
				open_table(pathname);
				fprintf(stderr,"open_table_finished\n");
				
			}
			//we got file
			fprintf(stderr,"lets rdo update log\n");
			fprintf(stderr,"again look for table\n");

			table = get_table_structure_by_name(pathname);
			if (table == NULL) {
				fprintf(stderr,"table_structure not set yet\n");
				free(log);
				exit(EXIT_FAILURE);
			}
			int page_number = getter_log_value_4byte(log, Page_Number);
			int64_t page_offset = page_number*page_size;
			buffer_structure* target_page = fetch(table->table_id, page_offset);
			fprintf(stderr,"we now got page\n");

			//check page_LSN
			int64_t page_LSN; memcpy(&page_LSN, target_page->frame + PAGE_LSN, 8);
			if (page_LSN > LOG_MANAGER->flushed_LSN) {
				printf("page is already new_version\n");
				//escape switch
				break;
			}


			int64_t absolute_offset = getter_log_value_8byte(log, OFFSET);
			printf("absolute offset is %ld\n", absolute_offset);

			char* old_image = (char*)malloc(sizeof(char) * 120);
			memcpy(old_image, log->frame + Old_Image,120);
			char* new_image = (char*)malloc(sizeof(char) * 120);
			memcpy(old_image, log->frame + New_Image, 120);
			char* disk_value = (char*)malloc(sizeof(char) * 120);
			memcpy(&disk_value, target_page->frame + absolute_offset,120);
			printf("disk value : %s\nredo(or undo) old_image: %s\n", disk_value,old_image);
			free(disk_value);

			if (!strcmp(old_image, disk_value)) {
				fprintf(stderr, "old_image == disk_value\n");
				memcpy(target_page->frame + absolute_offset, new_image, 120);
				
				//page LSN
				page_LSN = LOG_MANAGER->flushed_LSN;
				memcpy(target_page->frame + PAGE_LSN, &page_LSN, 8);

				//value and pageLSN was changed
				target_page->is_dirty = true;
			}

		}
		//switch finished
		//free log is required.
		free(log);
	}
	//while finished

	fprintf(stderr, "recovery finished\n");
	fprintf(stderr, "now flushed_LSN is %ld\n", LOG_MANAGER->flushed_LSN);

}

/* transaction functions */
//0
int begin_transaction() {
	new_transactional_unit();
	printf("hello\n");
	append_to_transaction(new_log(BEGIN));
	printf("auaicn\n");
	return 0;
}

//2
int commit_transaction() {
	//return 0 if success, otherwise return non-zero value
	//user can get response once all modification of transactions are flushed to a log file
	//If user get successfl return, that means your database can recover committed transaction after system crash
	append_to_transaction(new_log(COMMIT));
	flush_transaction();
	remove_transaction();
	return 0;
}

//3
int abort_transaction() {
	//return 0 if success, otherwise return non-zero value
	//All affected modification should be canceled and return to old state.
	printf("making undo information\n");

	log* redo_info= LOG_MANAGER->current_transaction->end;
	if (redo_info == NULL) {
		fprintf(stderr, "there is no current_transaction->end\n");
		exit(EXIT_FAILURE);
	}
	while (true) {//until begin log
		//check type
		if (getter_log_value_4byte(redo_info, Type) == BEGIN) {
			fprintf(stderr,"undo logging finished\n");
			break;
		}
		log* undo_log = new_log(UPDATE);
		memcpy(undo_log->frame, redo_info->frame,sizeof(undo_log->frame));
		char* old_value = (char*)malloc(sizeof(char) * 120);
		char* new_value = (char*)malloc(sizeof(char) * 120);
		memcpy(old_value, redo_info->frame + Old_Image, 120);
		memcpy(new_value, redo_info->frame + New_Image, 120);

		//swap and then write to undo_log and then append to the transaction object
		memcpy(undo_log->frame + Old_Image, new_value, 120);
		memcpy(undo_log->frame + New_Image, old_value, 120);

		free(old_value);
		free(new_value);

		append_to_transaction(undo_log);

	}
	append_to_transaction(new_log(ABORT));
	flush_transaction();
	remove_transaction();
	return 0;
}

void new_transactional_unit()
{
	transactional_unit* empty_transaction = (transactional_unit*)malloc(sizeof(transactional_unit));
	empty_transaction->begin = NULL;
	empty_transaction->end = NULL;
	empty_transaction->lastLSN = LOG_MANAGER->flushed_LSN; //initially 0
	empty_transaction->XID = transaction_count++; //start from 1
	printf("empty transaction with XID : %d made\n", empty_transaction->XID);
	LOG_MANAGER->current_transaction = empty_transaction;
}

log* new_log(int type) {
	log* new_log_ = (log*)malloc(sizeof(log));
	new_log_->frame = (char*)malloc(sizeof(char)*LOG_SIZE);
	memcpy(new_log_->frame + Type, &type, 4);
	return new_log_;
}

void new_log_manager()
{
	LOG_MANAGER = (log_manager*) malloc (sizeof(log_manager));
	LOG_MANAGER->current_transaction == NULL;
	char* pathname = "log.txt";
	if (access(pathname, F_OK) == -1) {
		//not accessable
		need_recovery = false;
		fprintf(stderr, "there is no log file. making \"log.txt in current directory\"\n");
		LOG_MANAGER->log_file = fopen(pathname, "w+");
	}
	else {
		fprintf(stderr, "there exists log file\n");
		fprintf(stderr, "opening log.txt\n");
		LOG_MANAGER->log_file = fopen(pathname,"r+");
	}
	LOG_MANAGER->flushed_LSN = 0;

}

int flush_transaction()
{
	printf("flushing transaction\n");
	transactional_unit* to_flush = LOG_MANAGER->current_transaction;
	if(to_flush==NULL){
		printf("there's no transaction\n");
		return 0;
	}
	log* begin = to_flush->begin;
	if(begin==NULL){
		printf("there's no begin log\n");
		return 0;
	}
	while (begin) {
		//victim 되어서 내려가는 거라면 LSN 상관없이 내려야하는데?
		//그럼에도 불구하고 log flush는 그대로인걸?
		//그래도 이렇게 짜면은 같은 log가 두번 내려갈 수 있겠다.
		if (getter_log_value_4byte(begin,LSN) < LOG_MANAGER->flushed_LSN) {
			//do not flush
			begin = begin->next;
			continue;
		}
		//append to the end
		fseek(LOG_MANAGER->log_file, 0, SEEK_END);
		fwrite(begin->frame, sizeof(char), LOG_SIZE, LOG_MANAGER->log_file);
		LOG_MANAGER->flushed_LSN += LOG_SIZE;
		begin = begin->next;
	}
	return 0;
}

int remove_transaction()
{
	transactional_unit* old_transaction = LOG_MANAGER->current_transaction;
	log* track = old_transaction->begin;
	log* remove_helper;
	if (track == NULL) {
		fprintf(stderr, "TRACK NULL POINTER \n");
		exit(EXIT_FAILURE);
	}
	while (track) {
		remove_helper = track;
		track = track->next;
		free(remove_helper);
	}
	LOG_MANAGER->current_transaction = NULL;
	return 0;
}

//1
int update(int table_id, int64_t key, char* new_value) {
	//Find the matching key and modify the value, where value size <= 120 bytes.
	//return 0 if success, otherwise return non-zero value
	//find_leaf(int table_id,int64_t key) {

	buffer_structure* target_page = find_leaf(table_id, key);

	//get index from leaf
	int index = 0;
	int number_of_keys = get_numKeys_of_page(target_page);

	int find = false;
	int64_t to_compare;
	while (index < number_of_keys) {

		to_compare = getter_leaf_key(target_page->frame, index);

		if (key == to_compare) {
			find = true;
			break;
		}
		index++;
	}

	if (!find) {
		fprintf(stderr, "Not Exists\n");
		return -1;
	}

	//make redo information
	char* old_value = getter_leaf_value(target_page->frame, index);
	new_value;
	log* new_log_ = new_log(UPDATE);
	memcpy(new_log_->frame + Old_Image, &old_value, 8);
	memcpy(new_log_->frame + New_Image, &new_value, 8);
	int page_number = target_page->page_LSN / 4096;
	memcpy(new_log_->frame + Page_Number, &page_number,4);
	int absolute_offset = header_page_size * leaf_key_record_size*index;
	memcpy(new_log_->frame + OFFSET, &absolute_offset, 8);
	memcpy(new_log_->frame + Table_ID, &table_id, 4);

	//renew pageLSN
	//caution some cheat with setting pageLSN
	//we are looking for current transaction's LSN+LOG_SIZE
	//because it will writen by append_to_transaction but not now

	int64_t old_pageLSN; memcpy(&old_pageLSN, target_page->frame + PAGE_LSN, 8);
	int64_t current_LSN; memcpy(&current_LSN,&(LOG_MANAGER->current_transaction->lastLSN), 8);
	current_LSN += LOG_SIZE;
	if (current_LSN > old_pageLSN);
		memcpy(target_page->frame + PAGE_LSN, &current_LSN, 8);

	//append redo log into current transaction
	append_to_transaction(new_log_);

	//write to buffer 
	setter_leaf_value(target_page->frame, index, new_value);

	target_page->is_dirty = true;
	target_page->pin_count--;
	return 0;
}

void append_to_transaction(log* new_log) {
	
	printf("initialize first and then\n");
	printf("appending to current transaction\n");

	//transaction has doublely linked list set
	memcpy(new_log->frame + transaction_ID, &(LOG_MANAGER->current_transaction->XID), 4);
	memcpy(new_log->frame + Prev_LSN, &(LOG_MANAGER->current_transaction->lastLSN), 8);
	LOG_MANAGER->current_transaction->lastLSN += LOG_SIZE;
	memcpy(new_log->frame + LSN, &(LOG_MANAGER->current_transaction->lastLSN), 8);

	printf("current lastLSN in transactional unit : %ld\n", LOG_MANAGER->current_transaction->lastLSN);
	log* search = LOG_MANAGER->current_transaction->begin;
	if (search == NULL){
		new_log->next = NULL;
		new_log->prev = NULL;
		LOG_MANAGER->current_transaction->begin
			= LOG_MANAGER->current_transaction->end = new_log;
	}
	else {
		new_log->next = NULL;
		new_log->prev = LOG_MANAGER->current_transaction->end;
		new_log->prev->next = new_log;
		LOG_MANAGER->current_transaction->end = new_log;
	}
	return;

}

/*returns table_id*/
int open_table(char* pathname) {
	//project 5 implementation
	//we limit the file name format as "DATA[NUM]" (for exaample, there should be
	//data files named like "DATA1", "DATA2",....)
	int data_file = false;
	
	char* filename_check = (char*)malloc(100);
	memcpy(filename_check, pathname, 4);
	filename_check[4] = '\0';
	if (!strcmp("DATA", filename_check))
		data_file = true;

	//Return value that indicates the table id should be NUM.

	FILE* fp;
	int fd;
	//check if already opened before
	table_structure* search = TABLE_MANAGER->tables;

	while (search != NULL) {
		//search by pathname
		if (strcmp(pathname, search->path) == 0) {
			current_table_id = search->table_id;
			current_fd = search->fd;
			buffer_structure* header = fetch(search->table_id, HEADER_PAGE_OFFSET);
			memcpy(&current_root_offset, header->frame + 8, 8);
			header->pin_count--;
			return search->table_id;
		}
		search = search->next;
	}

	fprintf(stderr, "not opened before\n");
	int there_is_no_file = true;

	//check if path is already occupied
	if (access(pathname, F_OK) == -1) {
		//file not exists
		fprintf(stderr, "file open fail so make new file : %s\n", strerror(errno));
		fp = fopen(pathname, "w+");
	}
	else {
		//file already exists
		//make table structure
		fprintf(stderr, "file exists so open the file\n");
		fprintf(stderr, "if the file do not follow the structure of db, it does not catch the error\n");
		there_is_no_file = false;
		//do not truncate
		fp = fopen(pathname, "r+");
	}

	//only know fp
	//not opened beforels

	//get table_id 
	//and fd from fd
	if (data_file) {
		char* data_file_table_id = (char*)malloc(100);
		memcpy(data_file_table_id, pathname + 4, 2);
		current_table_id = atoi(data_file_table_id);
	}
	else current_table_id = ++table_create_count;

	current_fd = fileno(fp);

	//make and initialize new_table_structure
	table_structure* new_table_structure = (table_structure*)malloc(sizeof(table_structure));

	new_table_structure->fd = current_fd;
	new_table_structure->table_id = current_table_id;
	memcpy(new_table_structure->path, pathname, strlen(pathname));

	//append to TABLE_MANAGER
	new_table_structure->next = TABLE_MANAGER->tables;
	TABLE_MANAGER->tables = new_table_structure;

	if (there_is_no_file)
		make_header_page(new_table_structure->table_id);

	//set current root offset
	buffer_structure* header = fetch(new_table_structure->table_id, HEADER_PAGE_OFFSET);
	memcpy(&new_table_structure->root_offset, header->frame + 8, 8);
	memcpy(&current_root_offset, header->frame + 8, 8);
	header->pin_count--;
	
	printf("new table created : it's table id is %d\n",new_table_structure->table_id);
	return new_table_structure->table_id;

}

int64_t getter_internal_key(char* frame, int index) {
	int64_t key; memcpy(&key, frame + header_page_size + internal_key_pointer_size*index, 8);
	return key;
}

int64_t getter_leaf_key(char* frame, int index) {
	int64_t key; memcpy(&key, frame + header_page_size + leaf_key_record_size*index, 8);
	return key;
}

char* getter_leaf_value(char* frame, int index) {
	char* value = (char*)calloc(120, sizeof(char));
	memcpy(value, frame + header_page_size + leaf_key_record_size*index + 8, 120);
	return value;
}

void setter_leaf_value(char*frame, int index, char* new_value) {
	memcpy(frame + header_page_size + leaf_key_record_size*index + 8, new_value, 120);
}

table_structure* is_opened(int table_id) {

	table_structure* search = TABLE_MANAGER->tables;

	while (search != NULL)
		if (search->table_id == table_id)
			break;
		else
			search = search->next;

	return search;

}

int join_table(int table1_id, int table2_id, const char* save_path) {


	//char buf[page_size*10];
	// file open
	if (is_opened(table1_id) == NULL || is_opened(table2_id) == NULL) {

		fprintf(stderr, "cannot find matching table_id\n");
		return -1;
	}

	/* path already exists */
	if (access(save_path, F_OK) == 0) {

		fprintf(stderr, "save path already exists\"\n");
		return -1;

	}

	FILE* output = fopen(save_path, "w+");
	setvbuf(output, NULL, _IOFBF, page_size * 1000);
	//joining R |><| S
	int i, j;
	buffer_structure* table1_ptr;
	buffer_structure* table2_ptr;
	int64_t next_offset;
	table1_ptr = find_leaf(table1_id, LONG_MIN);


	//loop for [table1] times
	while (table1_ptr != NULL) {

		int numKeys_table1 = get_numKeys_of_page(table1_ptr);

		//each key in a table1's block
		for (i = 0; i < numKeys_table1; i++) {

			int64_t join_key = getter_leaf_key(table1_ptr->frame, i);
			char* join_value = getter_leaf_value(table1_ptr->frame, i);

			printf("%ld\n", join_key);
			//loop for [table2] times
			table2_ptr = find_leaf(table2_id, LONG_MIN);

			while (table2_ptr != NULL) {
				int numKeys_table2 = get_numKeys_of_page(table2_ptr);
				//each key in a table2's block

				for (j = 0; j < numKeys_table2; j++) {
					int64_t compare_key = getter_leaf_key(table2_ptr->frame, j);

					if (join_key == compare_key) {

						char* value2 = getter_leaf_value(table1_ptr->frame, j);

						fprintf(output, "%" PRId64", %s, %" PRId64", %s\n", join_key, join_value, compare_key, value2);

						free(value2);

					}

				}

				memcpy(&next_offset, table2_ptr->frame + header_page_size - 8, 8);
				table2_ptr->pin_count--;

				if (next_offset == 0) break;
				table2_ptr = fetch(table2_id, next_offset);

			}

			free(join_value);

		}
		//looking for next buffer block table1
		/*From now on table1 may be victim.*/
		memcpy(&next_offset, table1_ptr->frame + header_page_size - 8, 8);
		table1_ptr->pin_count--;

		if (next_offset == 0) break;
		table1_ptr = fetch(table1_id, next_offset);

	}

	fclose(output);

	printf("check explicitly if file %s is in working directory!\n", save_path);

}

//struct constructor
void new_buffer_manager(int buffer_size) {

	buffer_manager* new_buffer_manager_ = (buffer_manager*)malloc(sizeof(buffer_manager));


	new_buffer_manager_->header = (buffer_structure*) malloc(sizeof(buffer_structure));
	new_buffer_manager_->header->next = NULL;
	new_buffer_manager_->header->prev = NULL;

	new_buffer_manager_->num_max_blocks = buffer_size;
	new_buffer_manager_->num_current_blocks = 0;

	BUFFER_MANAGER = new_buffer_manager_;

}

/* it needs table_id argument because it's used in ask_buffer call in join operation */
buffer_structure* new_buffer_block(int table_id, int64_t offset) {

	table_structure* table = get_table_structure(table_id);

	if (table == NULL) {
		fprintf(stderr, "new buffer block allocation failed\n");
		return NULL;
	}


	buffer_structure* new_buffer = (buffer_structure*)malloc(sizeof(buffer_structure));
	new_buffer->frame = (char*)malloc(page_size);

	pread(table->fd, new_buffer->frame, page_size, offset);// pread only here

														   //for now....
	new_buffer->next = NULL;
	new_buffer->prev = NULL;

	new_buffer->table_id = table_id;
	new_buffer->is_dirty = 0;
	new_buffer->pin_count = 0;
	new_buffer->page_offset = offset;

	return new_buffer;

}

void new_table_manager()
{
	TABLE_MANAGER = (table_manager*)malloc(sizeof(table_manager));
	TABLE_MANAGER->tables = NULL;
}

//normal definitions

void shutdown_db() {

	table_structure* killer = TABLE_MANAGER->tables;
	table_structure* killer_killer;

	while (killer != NULL) {
		killer_killer = killer;
		close_table(killer->table_id);
		free(killer_killer);
		killer = killer->next;
	}

}

/* display needs much pinned buffer */
void display(int table_id) {
	printf("------------------------display------------------------\n");

	buffer_structure* header = fetch(table_id, HEADER_PAGE_OFFSET);
	if (header == NULL) {
		fprintf(stderr, "asking headerpage that matches table_id failed\ncoding again\n");
		exit(EXIT_FAILURE);
	}
	int64_t root_offset; memcpy(&root_offset, header->frame + 8, 8);

	printf("current root offset : %ld\n", root_offset / 4096);

	//check empty
	if (root_offset == 0) {
		printf("empty tree.\n");
		return;
	}

	//get numPages to initialize queue_max_size
	int64_t numpages; memcpy(&numpages, header->frame + 16, 8);

	//header can be victim
	header->pin_count--;

	//initialize queue
	ENTRY *queue = (ENTRY*)calloc(numpages, sizeof(struct entry));
	int64_t queue_front = 0;
	int64_t queue_end = -1;

	//first element
	ENTRY root_;
	root_.page = fetch(table_id, root_offset);
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

		int numKeys; memcpy(&numKeys, pos.page->frame + 12, 4);
		//print key
		int leaf_key_record_size_ = test_leaf(pos.page) ? 128 : 16;

		///print looking page
		printf("|(p%" PRId64") ", pos.page->page_offset / 4096);

		int i;
		int64_t temp;

		for (i = 0; i < numKeys; i++) {
			memcpy(&temp, pos.page->frame + header_page_size + leaf_key_record_size_*i, 8);
			printf("%" PRId64" ", temp);
		}

		///enqueue others
		//not for leaf
		if (test_leaf(pos.page)) {
			pos.page->pin_count--;
			continue;
		}

		//for internal
		for (i = 0; i<numKeys + 1; i++) {
			++queue_end;
			memcpy(&temp, pos.page->frame + header_page_size - 8 + 16 * i, 8);
			queue[queue_end].page = fetch(table_id, temp);
			queue[queue_end].depth = pos.depth + 1;
		}
		pos.page->pin_count--;

	}
	printf("|\n");

	free(queue);

}

void bl_insert(buffer_structure* target, buffer_structure* prev_) {

	target->next = prev_->next;
	target->prev = ((prev_->next == NULL) || (prev_->next->prev == NULL)) ? NULL : prev_;

	(prev_->next == NULL ? BUFFER_MANAGER->header : prev_->next)->prev = target;
	prev_->next = target;

}

void bl_delete(buffer_structure* target) {

	(target->next == NULL ? BUFFER_MANAGER->header : target->next)->prev = target->prev;
	(target->prev == NULL ? BUFFER_MANAGER->header : target->prev)->next = target->next;

}

/*it returns buffer_structure_block with pincount plused*/
buffer_structure* fetch(int table_id, int64_t offset) {

	//start from MRU
	buffer_structure* search = BUFFER_MANAGER->header;

	//MRU NULL
	if (search->next == NULL) {

		buffer_structure* first_buffer_block = new_buffer_block(table_id, offset);
		
		if(first_buffer_block==NULL){
			fprintf(stderr,"fetch fail");
			return NULL;
		}
		bl_insert(first_buffer_block, BUFFER_MANAGER->header);

		BUFFER_MANAGER->num_current_blocks++;

		//printf("num_current_blocks changed to %d\n", BUFFER_MANAGER->num_current_blocks);

		//pin control
		first_buffer_block->pin_count++;

		return first_buffer_block;
	}

	//MRU NOT NULL
	//at least one element in the list
	for (; search != NULL; search = search->next) {

		if (!memcmp(&search->page_offset, &offset, 8) && table_id == search->table_id) {

			//printf("cache hit\n");

			bl_delete(search);
			bl_insert(search, BUFFER_MANAGER->header);

			search->pin_count++;
			return search;

		}

	}

	//printf("cache not hit\n");
	if (search == NULL) {

		buffer_structure* new_block = new_buffer_block(table_id, offset);

		if(new_block == NULL){
			fprintf(stderr,"fetch fail");
			return NULL;
		}
		bl_insert(new_block, BUFFER_MANAGER->header);

		if (BUFFER_MANAGER->num_current_blocks == BUFFER_MANAGER->num_max_blocks) {
			//find victim. need not but just implemented.
			//victime은 LRU 부터 search
			buffer_structure* victim = BUFFER_MANAGER->header->prev; //Never be NULL but it's prev can be NULL

			while (victim->pin_count != 0 && victim != NULL)
				victim = victim->prev;

			if (victim == NULL) {
				fprintf(stderr, "pin dead lock\n");
				exit(EXIT_FAILURE);
			}

			bl_delete(victim);

			flush_page(victim);
		}
		else {

			BUFFER_MANAGER->num_current_blocks++;
			//printf("num_current_blocks changed to %d\n", BUFFER_MANAGER->num_current_blocks);

		}

		new_block->pin_count++;
		return new_block;
	}

}


void close_table(int table_id_) {

	buffer_structure* ptr;
	for (ptr = BUFFER_MANAGER->header->next; ptr != NULL; ptr = ptr->next) {

		buffer_structure* current_block_ptr = ptr;

		if (ptr->table_id == table_id_) {

			bl_delete(ptr);

			flush_page(current_block_ptr);

			fdatasync(get_table_structure(table_id_)->fd);

			BUFFER_MANAGER->num_current_blocks--;

			//printf("num_current_blocks changed to %d\n", BUFFER_MANAGER->num_current_blocks);

		}
	}
}

void show_tables()
{
	printf("%12s%5s %-50s\n", "table_id", "fd", "path");
	table_structure* search = TABLE_MANAGER->tables;
	while (search != NULL) {
		printf("%12d%5d %-50s\n", search->table_id, search->fd, search->path);
		search = search->next;
	}
}

log* read_one_log() {
	//according to the flushedLSN, get log record in log.txt file.
	//frame is copied so type is not important
	log* one_record = new_log(BEGIN);
	printf("reading log\n");
	pread(fileno(LOG_MANAGER->log_file), one_record->frame, LOG_SIZE, LOG_MANAGER->flushed_LSN);
	LOG_MANAGER->flushed_LSN += LOG_SIZE;
	printf("flushedLSN increased to %ld\n", LOG_MANAGER->flushed_LSN);
	return one_record;
}

void init_db(int buffer_size) {

	init();
	//initialize LOG MANAGER
	//need_recovery is determined here. default value is true
	printf("call new log manager\n");
	new_log_manager();

	//may recovery is here
	if (need_recovery)
		recovery();
	else
		printf("need not recovery\n");

	printf("call new table manager\n");

	//initialize TABLE MANAGER
	new_table_manager();

	//initialize BUFFER_MANAGER and assign
	new_buffer_manager(buffer_size);

	fprintf(stderr, "%d is set as buffer_pool max size.\n", buffer_size);
	//printf("%d is set as buffer_pool max size.\n",buffer_size);

}

/*
sync outside
free inside
*/
void flush_page(buffer_structure* block) {
	fprintf(stderr, "flushing page and transaction\n");
	//we may not have to flush because the page is not dirty
	//LOG_MANAGER->current_transaction cannot be NULL
	//but we are not removing log from memory because they can be aborted.
	flush_transaction();

	if (block->is_dirty){
		printf("dirty page disk write\n");
		pwrite((get_table_structure(block->table_id))->fd, block->frame, 4096, block->page_offset);
	}

	free(block);

}


/* pin count dec but not freed */
void return_to_free_page(buffer_structure* page) {

	buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);

	int64_t first_free_page; memcpy(&first_free_page, header->frame, 8);

	memcpy(page->frame, &first_free_page, 8);
	memcpy(header->frame, &page->page_offset, 8);

	page->is_dirty = true;
	header->is_dirty = true;

	//page may be a victim
	page->pin_count--;
	header->pin_count--;

}

int adjust_root(buffer_structure* current_root) {
	//printf("adjusting root\n");

	// key was removed in higher recursive call
	// numKeys may be 0 (empty root)

	int numKeys; memcpy(&numKeys, current_root->frame + 12, 4);

	if (numKeys) {
		current_root->pin_count--;
		return 0;
	}

	else {

		if (test_leaf(current_root)) {
			//destroy tree
			return_to_free_page(current_root);

			buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);
			memcpy(header->frame + 8, &zero, 8);

			header->is_dirty = true;

			header->pin_count--;
			current_root->pin_count--;

			current_root_offset = 0;

		}
		else {
			//root is internal - it must have child

			//new root is leftmost child of current root
			int64_t new_root_offset; memcpy(&new_root_offset, current_root->frame + header_page_size - 8, 8);
			buffer_structure* new_root = fetch(current_table_id, new_root_offset);

			//pin count control will be handled in next call
			return_to_free_page(current_root);

			//register new root into global variable
			current_root_offset = new_root->page_offset;

			//ask header for register
			buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);

			//register new root into header page
			memcpy(header->frame + 8, &current_root_offset, 8);

			//new root has no parent
			memcpy(new_root->frame, &zero, 8);

			header->is_dirty = true;
			new_root->is_dirty = true;

			header->pin_count--;
			new_root->pin_count--;
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

			buffer_structure* child = fetch(current_table_id, child_offset);
			memcpy(child->frame, &neighbor_page->page_offset, 8);

			child->pin_count--;
			child->is_dirty = true;

		}

		//cpy elements except prime key
		memcpy(neighbor_page->frame + header_page_size + 16 * numKeys_neighbor
			, &prime_key, 8);
		memcpy(neighbor_page->frame + header_page_size + 16 * numKeys_neighbor + 8
			, page->frame + header_page_size - 8
			, 16 * numKeys_page + 8);
		numKeys_neighbor += 1; //important
	}
	else {

		//coalesce leaf pages. so no child control needed

		//cpy parent
		memcpy(neighbor_page->frame + header_page_size - 8
			, page->frame + header_page_size - 8, 8);

		//cpy elements
		memcpy(neighbor_page->frame + header_page_size + leaf_key_record_size*numKeys_neighbor
			, page->frame + header_page_size
			, leaf_key_record_size*numKeys_page);

	}

	return_to_free_page(page);

	numKeys_neighbor += numKeys_page;
	memcpy(neighbor_page->frame + 12, &numKeys_neighbor, 4);

	neighbor_page->pin_count--;
	neighbor_page->is_dirty = true;

	return delete_entry(parent, prime_key); //prime_key down.

}

int64_t getter_log_value_8byte(log* single_log, int offset){
	if(single_log==NULL) {
		fprintf(stderr,"single log is NULL\n");
		exit(EXIT_FAILURE);
	}
	int64_t temp; memcpy(&temp,single_log->frame+offset,8);
	return temp;
}

int getter_log_value_4byte(log* single_log, int offset){
	if(single_log==NULL) {
		fprintf(stderr,"single log is NULL\n");
		exit(EXIT_FAILURE);
	}
	int temp; memcpy(&temp,single_log->frame+offset,4);
	return temp;
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
			memmove(page->frame + header_page_size + leaf_key_record_size, page->frame + header_page_size, leaf_key_record_size*get_numKeys_of_page(page));

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
			buffer_structure* child = fetch(current_table_id, child_offset);
			memcpy(child->frame, &page->page_offset, 8);

			child->is_dirty = true;
			child->pin_count--;

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
			buffer_structure* child = fetch(current_table_id, child_offset);
			memcpy(child->frame, &page->page_offset, 8);

			child->is_dirty = true;
			child->pin_count--;

		}
		else {
			memcpy(page->frame + header_page_size + leaf_key_record_size*numKeys_page
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
				neighbor_page->frame + header_page_size + leaf_key_record_size
				, 128 * (numKeys_neighbor - 1));

	}

	numKeys_neighbor--; memcpy(neighbor_page->frame + 12, &numKeys_neighbor, 4);
	numKeys_page++; memcpy(page->frame + 12, &numKeys_page, 4);

	parent->is_dirty = true;
	neighbor_page->is_dirty = true;
	page->is_dirty = true;

	parent->pin_count--;
	neighbor_page->pin_count--;
	page->pin_count--;

	return 0;

}

int get_neighbor_index(buffer_structure* parent, buffer_structure* page) {

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

	if (page->page_offset == current_root_offset) { //deletion at root -> maybe root has to be disappeared.
		return adjust_root(page);
	}

	int min_keys = test_leaf(page) ? cut(get_order_of_current_page(page) - 1) : cut(get_order_of_current_page(page)) - 1;

	if (get_numKeys_of_page(page) >= min_keys) {
		page->pin_count--;
		return 0;
	}

	//get parent of page
	int64_t parent_offset; memcpy(&parent_offset, page->frame, 8);
	buffer_structure* parent = fetch(current_table_id, parent_offset);

	//get neighbor_index & prime_key & numKeys
	int neighbor_index = get_neighbor_index(parent, page); // [0-numKeys] not [-1 to numKeys-1];
	int prime_key_index = neighbor_index == -1 ? 0 : neighbor_index;// [0-(numKeys -1)]
	int temp = (neighbor_index == -1) ? 1 : neighbor_index;


	int64_t neighbor_offset; memcpy(&neighbor_offset, parent->frame + header_page_size - 8 + 16 * temp, 8);
	buffer_structure* neighbor = fetch(current_table_id, neighbor_offset);

	//page와 neighbor_page 사이의 key값을 구한다.
	int64_t prime_key; memcpy(&prime_key, parent->frame + header_page_size + 16 * prime_key_index, 8);

	// 31 for leaf and 248 for internal
	int capacity = get_order_of_current_page(page) - 1;

	if (get_numKeys_of_page(neighbor) + get_numKeys_of_page(page) <= capacity)
		return coalesce_pages(parent, page, neighbor, neighbor_index, prime_key);
	else
		return redistribute_pages(parent, page, neighbor, neighbor_index, prime_key_index, prime_key);
	return 0;

}

int delete(int table_id, int64_t key) {

	//char* key_record = find(get_table_id(), key);

	buffer_structure* leaf = find_leaf(table_id, key);

	if (leaf == NULL)
		return -1;
	else
		return delete_entry(leaf, key);

}

int remove_entry_from_page(buffer_structure* page, int64_t key) {

	int64_t to_compare;
	int64_t keyPointer_or_keyRecord_size = test_leaf(page) ? leaf_key_record_size : 16;

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
		page->pin_count--;
		return -1;
	}

	//key found

	memmove(page->frame + header_page_size + i*keyPointer_or_keyRecord_size
		, page->frame + header_page_size + (i + 1)*keyPointer_or_keyRecord_size
		, (numKeys - 1 - i)*keyPointer_or_keyRecord_size);

	numKeys--;
	memcpy(page->frame + 12, &numKeys, 4);

	page->is_dirty = true;
	//maybe we can set the other pointers and keys to null for tidiness

	return 0;
}

int insert_into_node_after_splitting(buffer_structure* left, int left_index
	, int64_t new_key, buffer_structure* right_child) {
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

		buffer_structure* child = fetch(current_table_id, child_offset);

		memcpy(child->frame
			, &right->page_offset
			, 8);

		child->pin_count--;

	}

	//set right's parent
	int64_t parent_of_left; memcpy(&parent_of_left, left->frame, 8);
	memcpy(right->frame, &parent_of_left, 8);


	left->is_dirty = true;
	right->is_dirty = true;

	right_child->pin_count--;

	free(temp);
	//display();
	insert_into_parent(left, right, prime_key);
}

int insert_into_node(buffer_structure* parent, int left_index,
	int64_t new_key, buffer_structure* right) {

	int64_t absolute_insertion_offset = header_page_size + 16 * left_index;

	// get numKeys of parent
	int numKeys;
	memcpy(&numKeys, parent->frame + 12, 4);

	// move 
	memmove(parent->frame + absolute_insertion_offset + 16
		, parent->frame + absolute_insertion_offset
		, 16 * (numKeys - left_index));

	// overwrite new key and offset
	memcpy(parent->frame + absolute_insertion_offset, &new_key, 8);
	memcpy(parent->frame + absolute_insertion_offset + 8, &right->page_offset, 8);

	// inc numKeys of parent
	numKeys++;
	memcpy(parent->frame + 12, &numKeys, 4);

	// modification control
	parent->is_dirty = true;

	// pin count control
	right->pin_count--;
	parent->pin_count--;


	return 0;
}


int insert_into_new_root(buffer_structure* left, int64_t key, buffer_structure* right) {

	// called becuase left's parent was 0

	buffer_structure* new_root = make_internal_page();

	//renew number of keys
	int number_of_keys;
	memcpy(&number_of_keys, new_root->frame + 12, 4); //now 0
	number_of_keys++;
	memcpy(new_root->frame + 12, &number_of_keys, 4); //now 1

													  // set parent offset to 0
	memcpy(new_root->frame, &zero, 8);
	memcpy(left->frame, &new_root->page_offset, 8);
	memcpy(right->frame, &new_root->page_offset, 8);

	// set key, pointer 
	memcpy(new_root->frame + 120, &left->page_offset, 8);
	memcpy(new_root->frame + 128, &key, 8);
	memcpy(new_root->frame + 136, &right->page_offset, 8);

	// renew global root.
	current_root_offset = new_root->page_offset;
	buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);
	memcpy(header->frame + 8, &current_root_offset, 8);

	// left , right modification control is alredy done.
	new_root->is_dirty = true;
	header->is_dirty = true;

	// pin count control
	new_root->pin_count--;
	left->pin_count--;
	right->pin_count--;
	header->pin_count--;

}


int insert_into_parent(buffer_structure* left, buffer_structure* right, int64_t new_key) {

	//offset of parent
	int64_t parent_offset; memcpy(&parent_offset, left->frame, 8);

	if (parent_offset == 0) {
		return insert_into_new_root(left, new_key, right);
	}

	buffer_structure* parent = fetch(current_table_id, parent_offset);


	//get parent's index that points left
	int left_index = 0;

	int64_t to_compare;
	while (left_index < get_numKeys_of_page(parent)) {

		to_compare = getter_internal_key(parent->frame, left_index);

		if (new_key > to_compare)
			left_index++;
		else
			break;

	}

	//following calls do not need left
	left->pin_count--;

	if (get_numKeys_of_page(parent) < get_order_of_current_page(parent) - 1)
		insert_into_node(parent, left_index, new_key, right);
	else
		insert_into_node_after_splitting(parent, left_index, new_key, right);

}


int insert_into_leaf_after_splitting(buffer_structure* leaf, int64_t key, char* value) {

	int insertion_point = 0;
	int order_ = get_order_of_current_page(leaf);

	// to save existing key and new key in ascending order
	char* temp = (char*)calloc(order_, 128);

	// split used after saving into temp
	int split = cut(order_);

	buffer_structure* new_leaf = make_leaf_page();

	// new leaf parent control
	memcpy(new_leaf->frame, leaf->frame, 8);

	// leaf - new_leaf

	int64_t to_compare;
	while (insertion_point < order_ - 1) {

		to_compare = getter_leaf_key(leaf->frame, insertion_point);

		if (key > to_compare)
			insertion_point++;
		else
			break;

	}

	//Not exists
	if (key == to_compare) {
		fprintf(stderr, "duplicate key\n");
		free(temp);
		return -1;
	}

	//copy to temp
	memcpy(temp, leaf->frame + header_page_size, insertion_point*leaf_key_record_size);
	memcpy(temp + insertion_point*leaf_key_record_size, &key, 8);
	memcpy(temp + insertion_point*leaf_key_record_size + 8, value, 120);
	memcpy(temp + (insertion_point + 1)*leaf_key_record_size
		, leaf->frame + header_page_size + insertion_point*leaf_key_record_size
		, (order_ - 1 - insertion_point)*leaf_key_record_size);

	//temp to each node left & right
	memcpy(leaf->frame + header_page_size, temp, leaf_key_record_size*split);
	memcpy(new_leaf->frame + header_page_size
		, temp + leaf_key_record_size*split
		, leaf_key_record_size*(order_ - split));

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

	//modification control
	leaf->is_dirty = true;
	new_leaf->is_dirty = true;

	//pin count control shifted to next call
	insert_into_parent(leaf, new_leaf, new_key);

}


int insert_into_leaf(buffer_structure* leaf, int64_t key, char* value) {
	//printf("pushing into leaf : p.%" PRId64 "\n", leaf->page_offset/4096);

	int insertion_point = 0;
	int num = get_numKeys_of_page(leaf);

	int64_t to_compare;
	while (insertion_point<num) {

		to_compare = getter_leaf_key(leaf->frame, insertion_point);

		if (to_compare < key)
			insertion_point++;
		else
			break;

	}

	//Exists
	if (to_compare == key) {
		leaf->pin_count--;
		fprintf(stderr, "duplicate key\n");
		return -1;
	}

	int absolute_insertion_offset = header_page_size + insertion_point * 128;

	memmove(leaf->frame + absolute_insertion_offset + 128,
		leaf->frame + absolute_insertion_offset,
		128 * (num - insertion_point));

	num++; memcpy(leaf->frame + 12, &num, 4);

	memcpy(leaf->frame + absolute_insertion_offset, &key, 8);
	memcpy(leaf->frame + absolute_insertion_offset + 8, null120, 120);
	memcpy(leaf->frame + absolute_insertion_offset + 8, value, 120);


	//modification control
	leaf->is_dirty = true;

	//pin count control
	leaf->pin_count--;

	return 0;
}

int insert(int table_id, int64_t key, char* value) { //사실 root가 전역변수라서 return 해줘야 하는지도 모르겠다.

	buffer_structure* header_page = fetch(table_id, HEADER_PAGE_OFFSET);
	int64_t root_offset; memcpy(&root_offset, header_page->frame + 8, 8);

	if (root_offset == 0) {
		// create tree

		buffer_structure* new_root = make_leaf_page();

		// register new_root_offset into header_page
		memcpy(header_page->frame + 8, &new_root->page_offset, 8);

		// initialize new_root
		// parent, new key, new value
		memcpy(new_root->frame, &zero, 8);
		memcpy(new_root->frame + header_page_size, &key, 8);
		memcpy(new_root->frame + header_page_size + 8, value, 120);

		//numKeys 0 -> 1
		int numKeys; memcpy(&numKeys, new_root->frame + 12, 4);
		numKeys++; memcpy(new_root->frame + 12, &numKeys, 4);

		//set global variable
		current_root_offset = new_root->page_offset;

		new_root->is_dirty = true;

		//victim available
		header_page->pin_count--;
		new_root->pin_count--;

		return 0;

	}

	header_page->pin_count--;

	//pin count handled later
	buffer_structure* leaf = find_leaf(table_id, key);

	//to erase
	//fprintf(stderr,"leaf offset : %ld\n",leaf->page_offset/4096);

	if (leaf == NULL) {
		fprintf(stderr, "insertion error\n");
		return -1;
	}

	//get max number of keys of leaf page
	int max_numKeys = get_order_of_current_page(leaf) - 1;

	// leaf split or not
	if (get_numKeys_of_page(leaf) < max_numKeys)
		return insert_into_leaf(leaf, key, value);
	else
		return insert_into_leaf_after_splitting(leaf, key, value);

}

int get_numKeys_of_page(buffer_structure* page) {
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

buffer_structure*
find_leaf(int table_id, int64_t key) {

	// set current global variables with table_id
	if (table_id != current_table_id) {
		table_structure* table = get_table_structure(table_id);
		if (table) {
			current_table_id = table_id;
			current_fd = table->fd;
			current_root_offset = table->root_offset;
		}
		else
			return NULL;
	}

	if (current_root_offset == 0) {
		fprintf(stderr, "cannot find leaf (tree empty)\n");
		return NULL;
	}

	//ask for root buffer_structure
	int64_t search_offset = current_root_offset;
	buffer_structure* search = fetch(current_table_id, search_offset);

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
		//printf("searching into : %" PRId64"\n",search_offset);

		search->pin_count--;
		search = fetch(current_table_id, search_offset);
	}

	return search;

	//search can be NULL?

}

char* find(int table_id, int64_t key) {

	int find = false;

	buffer_structure* leaf = find_leaf(table_id, key);

	if (leaf == NULL) return NULL;

	int number_of_keys;
	memcpy(&number_of_keys, leaf->frame + 12, 4);

	//linear seach get index of finding key
	int i = 0;
	int64_t to_compare;
	while (i < number_of_keys) {

		to_compare = getter_leaf_key(leaf->frame, i);

		if (key == to_compare) {
			find = true;
			break;
		}
		i++;
	}

	//if found, use i to extract value.
	leaf->pin_count--;
	if (find) {
		char* value_ = getter_leaf_value(leaf->frame, i);
		return value_;
	}
	else
		return NULL;

}

buffer_structure* make_leaf_page() {

	buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);

	//get free page offset
	int64_t new_leaf_offset;
	memcpy(&new_leaf_offset, header->frame, 8);

	buffer_structure* new_leaf = fetch(current_table_id, new_leaf_offset);

	fprintf(stderr, "making leaf page : no.%ld\n", new_leaf->page_offset / 4096);

	//filling first 16 bytes of new_leaf->frame
	memcpy(new_leaf->frame, &zero, 8);
	int leaf_ = 1;
	memcpy(new_leaf->frame + 8, &leaf_, 4);
	int number_of_key = 0;
	memcpy(new_leaf->frame + 12, &number_of_key, 4);

	make_free_page();

	//header not changed so need not make header dirty
	new_leaf->is_dirty = true;

	header->pin_count--;

	return new_leaf;

}

buffer_structure* make_internal_page() {
	//filling number of keys and is leaf
	buffer_structure* header = fetch(current_table_id, HEADER_PAGE_OFFSET);

	int64_t new_internal_offset;
	memcpy(&new_internal_offset, header->frame, 8);

	buffer_structure* new_internal = fetch(current_table_id, new_internal_offset);

	fprintf(stderr, "making internal page : no.%ld\n", new_internal->page_offset / 4096);

	//filling first 16 bytes of new_internal->frame
	memcpy(new_internal->frame, &zero, 8);
	int leaf_ = 0;
	memcpy(new_internal->frame + 8, &leaf_, 4);
	int number_of_key = 0;
	memcpy(new_internal->frame + 12, &number_of_key, 4);

	make_free_page();

	new_internal->is_dirty = true;
	//header not changed so need not make header dirty

	header->pin_count--;

	return new_internal;
}

void make_free_page() {

	buffer_structure* header_page = fetch(current_table_id, HEADER_PAGE_OFFSET);

	int64_t number_of_pages;
	memcpy(&number_of_pages, header_page->frame + 16, 8);

	int64_t new_free_page_offset = number_of_pages*page_size;
	buffer_structure* new_free_page = fetch(current_table_id, new_free_page_offset);

	memcpy(new_free_page->frame, header_page->frame, 8);
	memcpy(header_page->frame, &new_free_page_offset, 8);

	number_of_pages++;
	memcpy(header_page->frame + 16, &number_of_pages, 8);

	//modification control
	new_free_page->is_dirty = true;
	header_page->is_dirty = true;

	//now header and new free page can be a victim
	new_free_page->pin_count--;
	header_page->pin_count--;

	return;
}

void make_header_page(int table_id) {

	int64_t number_of_page = 1;

	buffer_structure* new_header = fetch(table_id, HEADER_PAGE_OFFSET);

	memcpy(new_header->frame, &zero, 8);
	memcpy(new_header->frame + 8, &zero, 8);
	memcpy(new_header->frame + 16, &number_of_page, 8);

	make_free_page();

	new_header->pin_count--;
}

void show_me_buffer() {
	buffer_structure* trace = BUFFER_MANAGER->header->next;
	while (trace != NULL) {

		fprintf(stderr, "|T%d p%ld (%d)", trace->table_id, trace->page_offset / 4096, trace->pin_count);
		trace = trace->next;
	}
	fprintf(stderr, "|\n");
	return;
}

table_structure* get_table_structure_by_name(char* pathname) {

	table_structure* t = TABLE_MANAGER->tables;
	while (t != NULL) {
		if (pathname == t->path)
			return t;
		t = t->next;
	}
	fprintf(stderr, "get table structure by name failed\n");
	return NULL;
}

table_structure* get_table_structure(int table_id) {

	table_structure* t = TABLE_MANAGER->tables;

	while (t != NULL) {
		if (table_id == t->table_id)
			return t;
		t = t->next;
	}
	fprintf(stderr, "get table structure failed\n");
	return NULL;

}

void init() {
	transaction_count = 1;
	need_recovery = true;
	null120 = (char*)calloc(120, sizeof(char));
	return;
}

void usage() {

	printf("\n");
	printf("+COPYRIGHT AND USAGE--------------------------------------------------------------------------\n");
	printf("|copyright on https://hconnect.hanyang.ac.kr/2017_ITE2038_11735/2017_ITE2038_2016024811.git \n");
	printf("|code represents single-processed, disk-based b+tree implementation\n");
	printf("|branch factor ( order )\n");
	printf("|leaf : 32 (numKeys will be 31)\n");
	printf("|internal : 249 (numKeys will be 248)\n");
	printf("|[Usage]\n");
	printf("|insert  :> i <table_id> <key> <value>\n");
	printf("|delete  :> d <table_id> <key>\n");
	printf("|find    :> f <table_id> <key>\n");
	printf("|quit    :> q\n");
	printf("|usage again! :> u\n");
	printf("|open table  :> o <path>\n");
	printf("|close table :> c <table_id>\n");
	printf("|transaction begin : 0\n");
	printf("|transaction update : 1 <table_id> <key> <new_value>\n");
	printf("|transaction commit : 2\n");
	printf("|transaction abort : 3\n");
	printf("|transaction structure check : 4\n");

	/*
	printf("|code includes 'insert' 'delete' 'find' 'display-tree' 'join' and other operations\n");
	printf("|display :> p <table_id>\n");
	printf("|tables  :> t\n");
	printf("|join    :> <table1_id> <table2_id> <save_path>\n");
	printf("|buffer-display :> b \n");
	*/
	printf("+---------------------------------------------------------------------------------------------\n");

}
