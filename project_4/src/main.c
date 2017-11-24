#include "bpt.h"

int main(void) {

    int table_id;
    int table2_id;
    int size;
    int64_t input;
    char instruction;
    char buf[120];
    char path[120];

 //   print_file();
//    init_db(20);
//    open_table("test.db");
//    printf("> ");
    while(scanf("%c", &instruction) != EOF){
        switch(instruction){
            case 'c':
                scanf("%d", &table_id);
                close_table(table_id);
//                print_buffer();
//                printf("close table %d!\n", table_id);
                break;
            case 'd':
                scanf("%d %ld", &table_id, &input);
                delete(table_id, input);
                break;
            case 'i':
                scanf("%d %ld %s", &table_id, &input, buf);
                insert(table_id, input, buf);
//                printf("after insert %ld\n", input);
//                print_file(1);
                break;
            case 'f':
                scanf("%d %ld", &table_id, &input);
                char * ftest;
                if((ftest = find(table_id, input)) != NULL){
                    printf("Key: %ld, Value: %s\n", input, ftest);
                    fflush(stdout);
                    free(ftest);
                }
                else{
                    printf("Not Exists\n");
                    fflush(stdout);
                }
                break;
            case 'j':
        		scanf("%d %d ",&table_id,&table2_id);
        		scanf("%s",path);
        		join(table_id,table2_id,path);
        		break;
            case 'l':
                scanf("%d", &table_id);
                //print_file(table_id);
                break;
            case 'n':
                scanf("%d", &size);
                init_db(size);
//                printf("Buffer setted!\n");
                break;
            case 'o':
                scanf("%s", path);
                table_id = open_table(path);
//                printf("%s table ID is: %d\n", path, table_id);
                break;
//            case 'p':
//                print_tree();
//                break;
            case 'q':
                shutdown_db();
                while(getchar() != (int64_t)'\n');
                return 0;
                break;
        }
        while(getchar() != (int)'\n');
//                printf("> ");
    }
    printf("\n");

    return 0;
}