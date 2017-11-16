#include "bpt.h"

int main(){
    int64_t input;
    char instruction;
    char buf[120];
    char *result;
    
   open_db("test.db");
    while(scanf("%c", &instruction) != EOF){
        switch(instruction){
            case 'i':
                scanf("%ld %s", &input, buf);
                insert(input, buf);
                
               break;
            case 'f':
                scanf("%ld", &input);
                result = find(input);
                if (result) {
                    printf("Key: %ld, Value: %s\n", input, result);
                } else
                    printf("Not Exists\n");

                fflush(stdout);
                break;
            case 'd':
                scanf("%ld", &input);
                delete(input);
                break;
            case 'q':
                while (getchar() != (int)'\n');
                return EXIT_SUCCESS;
                break;   

       }
        while (getchar() != (int)'\n');
    }
    printf("\n");
    return 0;
}
