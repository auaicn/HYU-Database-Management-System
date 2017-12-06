#include "bpt.h"
#include "join.h"

// MAIN
int main( int argc, char ** argv ) {
    int i = 0;
    char input_file[120];
    int input;
    char word[120];
    int fd[10];
    int p;
    int a, b;
    char join_file[120];
    int table_id;
    char instruction;
    setvbuf(stdin, NULL, _IONBF, 0);
    setvbuf(stdout, NULL, _IONBF, 0);
    int root = 0;
    init_db(16);
    // int fp = open_table("test.db");

    //printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'o':
            scanf("%s", input_file);
            fd[i] = open_table(input_file);
            printf("%d\n", fd[i]);
            i++;
            break;
        case 'a':
            scanf("%d", &input);
            init_db(input);
            break;
        case 'd':
            scanf("%d %d", &table_id, &input);
            root = delete(table_id, input);
            break;
        case 'i':
            scanf("%d %d %s", &table_id, &input, word);
            p = insert(table_id, input, word);
            // if(p == 0) printf("insert success\n");
            break;
        case 'f':
            scanf("%d %d", &table_id, &input);
            char *n = find(table_id, input);
            if(n != NULL){
              printf("Key: %d, Value: %s\n", input, n);
              free(n);
            }
            else{
              printf("Not Exists\n");
            }
            break;
        case 'j':
            scanf("%d %d %s", &a, &b, join_file);
            p = join_table(a, b, join_file);
            if(p == 0) printf("join success\n");
            break;
        case 'c':
        case 'q':
            while (getchar() != (int)'\n');
            int j;
            for(j = i - 1; j >= 0; j--)
              close_table(fd[j]);
            shutdown_db();
            return EXIT_SUCCESS;

            break;
        default:
            break;
        }
        while (getchar() != (int)'\n');
        //printf("> ");
    }
    printf("\n");

    return EXIT_SUCCESS;
}
