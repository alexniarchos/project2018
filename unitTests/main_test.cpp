#include "../join.h"
#include "../list.h"
#include <gtest/gtest.h>
#include <iostream>

int main(int argc, char **argv) {
    numofbuckets=1;
    for(int i=0;i<n_last_digits;i++){
        numofbuckets*=2;
    }
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}