if [[ $# < 1 ]]; then
    echo "Need PHP Path, such as /usr/local/php/"
    exit
fi

PHP_PATH=$1
PHP_IZE=$PHP_PATH/bin/phpize
PHP_EXE=$PHP_PATH/bin/php
PHP_CON=$PHP_PATH/bin/php-config

make -C ../libzp/libzp
$PHP_IZE
CXXFLAGS="-std=c++11 -g -O2" EXTRA_LDFLAGS="-lpthread" ./configure --with-php-config=$PHP_CON
make
