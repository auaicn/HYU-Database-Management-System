#include "bpt.h"

int main(int argc, const char* argv[]) {

	if (argc < 2){
		fprintf(stderr,"give max buffer size as argument\n");
		exit(EXIT_FAILURE);
	}

	int table_id;
	int table2_id;
	int64_t input;
	char instruction;
	char buf[120];
	char path[120];
	int begin;
	// no special function in init()
	// but for later implementation
	
	//usage();
	init();
	init_db(atoi(argv[1]));

	fprintf(stderr,"> ");
	while (scanf("%c", &instruction) != EOF) {
		switch (instruction) {
			case 'c':
				scanf("%d", &table_id);
				close_table(table_id);
				break;
			case 'd':
				scanf("%d %ld", &table_id, &input);
				delete(table_id, input);
				//display(table_id);
				break;
			case 'i':
				scanf("%d %ld %s", &table_id, &input, buf);
				fprintf(stderr,"inserting %ld\n",input);
				if (insert(table_id, input, buf) == -1)
					fprintf(stderr, "insertion error\n");
				//show_me_buffer();
				//display(table_id);
				//printf("num current blocks : %d\n",BUFFER_MANAGER->num_current_blocks);
				//printf("\n");
				break;
			case 'f':
				scanf("%d %ld", &table_id, &input);
				char * ftest = (char*)malloc(120);
				if ((ftest = find(table_id, input)) != NULL) {
					printf("Key: %ld, Value: %s\n", input, ftest);
					free(ftest);
				}
				else 
					printf("Not Exists\n");
				break;
			case 'u':
				usage();
				break;
			case 'j':
				scanf("%d %d %s", &table_id, &table2_id,path);
				if(join_table(table_id, table2_id, path)==-1)
					fprintf(stderr,"join failed\n");
				break;
			case 'o':
				scanf("%s", path);
				table_id = open_table(path);
				//printf("%s table ID is: %d\n", path, table_id);
				break;
			case 'p':
				scanf("%d", &	table_id);
				display(table_id);
				break;
			case 'q':
				shutdown_db();
				while (getchar() != (int64_t)'\n');
				printf("bye\n");
				return 0;
				break;
			case 't':
				show_tables();
				break;
			case 'b':
				show_me_buffer();
				break;
			case '0':
				if(begin) 
				{
					fprintf(stderr, "interleave error\n");
					exit(EXIT_FAILURE);
				} 
				else if(begin_transaction())
				{
					fprintf(stderr,"begin_transaction failed\n");
				}
				else 
					begin = true;
				break;
			case '1':
				scanf("%d %ld %s", &table_id, &input, buf);
				if(update(table_id,input,buf))
				{
					fprintf(stderr,"update failed\n");
				}
				break;
			case '2':
				if(commit_transaction())
				{
					fprintf(stderr,"commit_transaction failed\n");
				}
				begin = false;
				break;
			case '3':
				if(abort_transaction())
				{
					fprintf(stderr,"abort_transaction failed\n");
				}
				begin = false;
				break;
		}
		display_current_transaction_log_on_memory();
		while (getchar() != (int)'\n');
		fprintf(stderr,"> ");
	}
	//printf("\n");

	return 0;
}