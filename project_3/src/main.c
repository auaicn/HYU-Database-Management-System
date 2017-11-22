#include "bpt.h"

int main(int argc, const char* argv[]) {

	char command;
	char buf[120];
	char path[50];
	char *result;
	int opened = true;
	int64_t input;
	
	//make buffer
	/*
	if (argc == 1) {
		fprintf(stderr, "command line first argument error.\n");
		fprintf(stderr, "Usage : execute_command <buffer_limit>\n");
		exit(EXIT_FAILURE);
	}
	*/
	//init_db(atoi(argv[1]));
	init_db(10);

	//printf("> ");
	open_db("test.db");

	//make buffer end
	while (scanf("%c", &command) != EOF) {
		switch (command) {
			case 'i':
		        scanf("%" PRId64" %s", &input, buf);
				insert(get_table_id(), input, buf);
				//display();
				//show_me_buffer();
				break;
			case 'p':
				if (opened)
					display();
				break;
			case 'd':
				scanf("%ld" PRId64, &input);
				if (opened)
					delete(get_table_id(),input);
				display();
				break;
			case 'o':
				if (fd != 0) close_db(get_table_id());

				scanf("%s", path);
				open_db(path); //설정 된후 계속 바뀔 것이다.

				opened = true;
				break;
			case 'f':
				scanf("%" PRId64, &input);
				result = find(get_table_id(), input);

				if (result)
		            printf("Key: %" PRId64", Value: %s\n", input, result);
				else
					printf("Not Exists\n");

				break;
			case 'q':
				close_db(get_table_id());
				return EXIT_SUCCESS;
				break;
		}
		if (!opened)
			printf("operation cannot permitted. \"OPEN_DB\" should be preceded \n");
		while (getchar() != (int)'\n');
		//printf("> ");
	}
	return 0;
}
