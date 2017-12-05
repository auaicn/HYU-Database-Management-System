#include <stdio.h>
#include <stdlib.h>
#define MAX_FILE_PATH_SIZE 120

void usage() {
	fprintf(stderr, "USAGE : CMD <num> <mode> <path> <table_name> \n");
	exit(EXIT_FAILURE);
}

int main(int argc, const char* argv[]) {
	

	//error handling
	if (argc < 5){
		usage();
	}

	int count_ = atoi(argv[1]);
	char mode = argv[2][0];
	char* insert_value = (char*)calloc(sizeof(char), MAX_FILE_PATH_SIZE);
	insert_value[0] = 'v';

	FILE* f = fopen(argv[3], "w+");

	fprintf(f,"o %s\n",argv[4]);
	for (int i = 0; i < count_; i++) {
		
		fprintf(f, "%c %d %d ", mode, 1, i);

		if (mode == 'i') {
			sprintf(insert_value+1,"%d",i);
			fprintf(f, "%s ", insert_value);
		}

		fprintf(f,"\n");

	}

	fprintf(f,"q\n");
	fclose(f);

	printf("input file %s creadted.\n",argv[3]);
	printf("redirection %s to main with buffer size argument\n",argv[3]);
	printf("then you may get table named \"%s\"\n",argv[4]);
	exit(EXIT_SUCCESS);

}