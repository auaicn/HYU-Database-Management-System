#!/usr/bin/python3

'''
    test program
'''

from subprocess import Popen, PIPE
import os
from random import shuffle
from timeit import default_timer as tiimer
from math import log, floor
from time import sleep

INIT_CMD_FMTS = "n 20000\n"
OPEN_CMD_FMTS = "o test.db\n"
CLOSE_CMD_FMTS = "c 1\n"
INSERT_CMD_FMTS = "i 1 %d %s\n"
DELETE_CMD_FMTS = "d 1 %d\n"
FIND_CMD_FMTS = "f 1 %d\n"
FIND_RESULT_FMTS = "Key: %d, Value: %s"
NOT_FOUND_RESULT = "Not Exists"
RESULT_FMTS = "Result: %d/%d (%.2f) %.2f secs"


SMALL_CASE = 2 ** 10
MEDIUM_CASE = 2 ** 15
LARGE_CASE = 2 ** 20

def test_case(arr):
    os.system('rm -f test.db')
    sleep(0.1)
    f = open("log.txt", "w")
    p = Popen("./main", stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    # init buffer
    input_d = INIT_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    # open file
    input_d = OPEN_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    # insert start
    start = timer()

    for i in arr:
        input_d = INSERT_CMD_FMTS % (i, 'a' + str(i))
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()

    input_d = CLOSE_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    end = timer()

    input_d = OPEN_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    sleep(1)
    for i in arr:
        input_d = FIND_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == (FIND_RESULT_FMTS % (i, 'a' + str(i))).strip()):
            succ += 1

    p.stdin.write('q\n'.encode('utf-8'))
    f.close()
    p.stdin.flush()
    os.system("cp test.db last_test.db")

    return succ, end - start

def test_case_seq(casename, case_size):
    print(casename + " Test")

    result, elapse = test_case(range(case_size))

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()



def test_case_rnd(casename, case_size):
    print(casename + " Test")
    arr = list(range(case_size))
    shuffle(arr)

    result, elapse = test_case(arr)

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()

def test_delete(remain_rec, delete_rec):
    os.system('rm -f test.db')
    os.system('cp test.db.bak test.db')
    sleep(0.1)
    f = open("log.txt", "w")
    p = Popen("./main", stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    # init buffer
    input_d = INIT_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    input_d = OPEN_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    # insert start
    start = timer()

    for i in delete_rec:
        input_d = DELETE_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()

    input_d = CLOSE_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    end = timer()

    input_d = OPEN_CMD_FMTS
    f.write(input_d)
    p.stdin.write(input_d.encode("utf-8"))
    p.stdin.flush()

    remain_rec.sort()
    delete_rec.sort()
    for i in remain_rec:
        input_d = FIND_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == (FIND_RESULT_FMTS % (i, 'a' + str(i))).strip()):
            succ += 1

    for i in delete_rec:
        input_d = FIND_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == NOT_FOUND_RESULT):
            succ += 1
    p.stdin.write('q\n'.encode('utf-8'))
    p.stdin.flush()
    f.close()

    return succ, end - start

def test_delete_random(casename, pick):
    print(casename + " Test")
    arr = list(range(LARGE_CASE))
    shuffle(arr)

    result, elapse = test_delete(arr[pick:], arr[:pick])
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit()

def test_delete_seq():
    print("Delete All Records Sequantial")
    arr = list(range(LARGE_CASE))

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED Delete Seq - Stop Testing")
        exit()

def test_delete_rev():
    print("Delete All Records Reversal")
    arr = list(range(LARGE_CASE))
    arr.reverse()

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit()

def test_delete_chunk():
    print("Delete Chunk Records randomly")
    arr = list(range(LARGE_CASE))
    exp = floor(log(LARGE_CASE, 2) / 2)
    start = int(2 ** exp)
    end = int(2 ** (exp+1))

    deleted = arr[start: end]
    arr = arr[:start] + arr[end:]

    result, elapse = test_delete(arr, deleted)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit();



def make_backup():
    os.system('cp last_test.db test.db.bak')

os.system('make clean > /dev/null')
os.system('make main > /dev/null')

try:
    os.remove("test.db")
except:
    pass
print("-------------- Sequantial Insert Test --------------")
test_case_seq("Small(2^10)", SMALL_CASE)
test_case_seq("Medium(2^15)", MEDIUM_CASE)
test_case_seq("Large(2^20)", LARGE_CASE)


print("--------------   Random Insert Test   --------------")
test_case_rnd("Small(2^10)", SMALL_CASE)
test_case_rnd("Medium(2^15)", MEDIUM_CASE)
test_case_rnd("Large(2^20)", LARGE_CASE)
sleep(0.1)

make_backup()
sleep(0.1)
print("--------------      Delete Test       --------------")
test_delete_seq()
test_delete_rev()
test_delete_chunk()
test_delete_random("Small(2^10)", SMALL_CASE)
test_delete_random("Medium(2^10)", MEDIUM_CASE)
test_delete_random("Large(2^19)", int(LARGE_CASE / 2))
test_delete_random("ALL(2^20)", LARGE_CASE)

print("-------------- Sequantial Insert Test --------------")
test_case_seq("Small(2^10)", SMALL_CASE)
test_case_seq("Medium(2^15)", MEDIUM_CASE)
test_case_seq("Large(2^20)", LARGE_CASE)


print("???????   Random Insert Test   ???????")
test_case_rnd("Small(2^10)", SMALL_CASE)
test_case_rnd("Medium(2^15)", MEDIUM_CASE)
test_delete_random("Small(2^10)", SMALL_CASE)
test_case_rnd("Large(2^20)", LARGE_CASE)
test_delete_random("Medium(2^10)", MEDIUM_CASE)
sleep(0.1)