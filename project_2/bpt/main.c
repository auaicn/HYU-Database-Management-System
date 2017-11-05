int main(int argc, const char* argv[]) {
	char command;
	int64_t insert_key;
	int opened = false;
	printf("> ");
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