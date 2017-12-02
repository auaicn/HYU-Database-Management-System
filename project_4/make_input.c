#include <stdio.h>
#include <stdlib.h>
#define MAX_FILE_PATH_SIZE 120

void usage() {
	fprintf(stderr, "USAGE : CMD <table_id> <count> <mode> <path> \n");
	exit(EXIT_FAILURE);
}

int main(int argc, const char* argv[]) {
	
	//error handling
	if (argc < 5)
		usage();

	int table_id = atoi(argv[2]);
	int count_ = atoi(argv[3]);
	char mode = argv[4][0];
	char* path = (char*)calloc(sizeof(char), MAX_FILE_PATH_SIZE);
	char* insert_value = (char*)calloc(sizeof(char), MAX_FILE_PATH_SIZE);
	insert_value[0] = 'v';

	FILE* f = fopen(path, "w+");

	for (int i = 0; i < count_; i++) {
		
		fprintf(f, "%c %d %d ", mode, table_id, i);

		if (mode == 'i') {
			itoa(i, insert_value + 1, 10);
			fprintf(f, "%s ", insert_value);
		}

	}

	fprintf(stdout, "check path\n");
	exit(EXIT_SUCCESS);

}