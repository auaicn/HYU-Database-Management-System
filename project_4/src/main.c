#include "bpt.h"

int main(int argc, const char* argv[]) {

	int table_id;
	int table2_id;
	int size;
	int64_t input;
	char instruction;
	char buf[120];
	char path[120];

	// no special function in init()
	// but for later implementation
	init();

	init_db(atoi(argv[1]));
	usage();
	printf("%d is set as buffer_pool max size.\n",atoi(argv[1]));
	printf("> ");
	while (scanf("%c", &instruction) != EOF) {
		switch (instruction) {
		case 'c':
			scanf("%d", &table_id);
			close_table(table_id);
			show_me_buffer();
			break;
		case 'd':
			scanf("%d %ld", &table_id, &input);
			delete(table_id, input);
			break;
		case 'i':
			scanf("%d %ld %s", &table_id, &input, buf);
			if (insert(table_id, input, buf) == -1)
				printf("following key is not existing in leaf page\n");
			break;
		case 'f':
			scanf("%d %ld", &table_id, &input);
			char * ftest;
			if ((ftest = find(table_id, input)) != NULL) {
				printf("Key: %ld, Value: %s\n", input, ftest);
				fflush(stdout);
				free(ftest);
			}
			else {
				printf("Not Exists\n");
				fflush(stdout);
			}
			break;
		case 'u':
			usage();
			break;
		case 'j':
			scanf("%d %d ", &table_id, &table2_id);
			scanf("%s", path);
			join(table_id, table2_id, path);
			break;
		case 'n':
			scanf("%d", &size);
			init_db(size);
			printf("Buffer setted!\n");
			break;
		case 'o':
			scanf("%s", path);
			table_id = open_table(path);
			printf("%s table ID is: %d\n", path, table_id);
			break;
		case 'p':
			scanf("%d", table_id);
			display(table_id);
			break;
		case 'q':
			shutdown_db();
			while (getchar() != (int64_t)'\n');
			printf("bye\n");
			return 0;
			break;
		}
		while (getchar() != (int)'\n');
		//                printf("> ");
	}
	printf("\n");

	return 0;
}