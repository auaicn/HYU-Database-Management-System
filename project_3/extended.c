extern buffer_manager* BUFFER_MANAGER;

//declarations

//declarations



struct 
buffer_structure{
	char * frame;
	int table_id = 0;
	int64_t page_offset;
	int is_dirty = 0;
	int pin_count = 0;
	struct buffer_structure* next;
	struct buffer_structure* prev;
};



int ask_buffer_manager(int64_t offset){
	int * track = BUFFER_MANAGER->
}

struct 
buffer_manager{
	int num_current_blocks =0;
	int max_blocks = -1;
	buffer_structure* MRU;
}

void
close_db(){
	buffer_structure* ptr =BUFFER_MANAGER->MRU;
	while(ptr!=NULL){
		buffer_structure* current_block_ptr = ptr;
		ptr = ptr->next;
		//table_id check later
		update(current_block_ptr);
	}
	return;
}

void 
init_db(int buffer_size){
	//initialize BUFFER_MANAGER
	BUFFER_MANAGER = malloc(sizeof(buffer_manager));
	BUFFER_MANAGER->max_blocks = buffer_size;
	BUFFER_MANAGER->MRU = NULL;
}

void 
take_to_buffer_manager(int64_t offset){
	if(BUFFER_MANAGER->num_current_blocks<BUFFER_MANAGER->max_blocks){

	}

}

buffer_structure* new_buffer_block(int64_t offset){
	buffer_structure* new_buffer = (buffer_structure*) malloc(sizeof(buffer_structure));

	int
}

void
raise_to_buffer(int64_t page){

}

void
update(buffer_structure* block){
	pwrite(fd,block->frame,4096,block->page_offset);
	fdatasync(fd);
}

int 
main(int argc, const char* argv[]) {

	char command;
	int64_t insert_key;
	int opened = false;
	printf("> ");
	//make buffer
	if(argc==1){
		fprintf(stderr,"command line first argument error.\n");
		fprintf(stderr,"Usage : execute_command <buffer_limit>\n");
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
				insert(insert_key, value_);
			display();
			break;
		case 'd':
			if(opened)
				display();
			break;
		case 'r':
			scanf("%" PRId64, &insert_key);
			if (opened)
				delete_(insert_key);
			display();
			break;
		case 'o':
			printf("opening ");
			char absolute_path[200]; getcwd(absolute_path, 199);
			char path[50];	scanf("%s", path);
			printf("%s/%s\n", absolute_path, path);
			open_db(path); //설정 된후 계속 바뀔 것이다.
			opened = true;
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
			exit(EXIT_SUCCESS);
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

