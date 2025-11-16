#!/bin/env bash


URL='https://the-eye.eu/public/'

URL_NOW=$URL
MAX_LIMIT_TIMEOUT=3600
MAX_TIMOUT=60
FILE_DUMP_NOW=$(date +%s)
HOURS_TO_WAIT=1
SECONDS_TO_WAIT=$((HOURS_TO_WAIT * 3600))
LIST_LAST_LAYER=''

# 1. Save the original IFS value
OLD_IFS=$IFS

# 2. Set IFS to only the newline character
# The $'\n' syntax is the POSIX way to represent a newline.
IFS=$'\n'

echo 'timestamp,hash_sha256,url' ${FILE_DUMP_NOW}

while [ true ] ; do

    list_of_urls_now=$(curl -m ${MAX_LIMIT_TIMEOUT} --connect-timeout ${MAX_TIMOUT} "$URL_NOW" 2> /dev/null | grep '<a ' | grep href | cut -d' ' -f2 | cut -d'"' -f2 | grep '/$');
    # 3. Use the for loop to iterate over the 'ls' output
    for urinow in ${list_of_urls_now}; do
        # echo ${URL}${urinow}
        # sleep 4
        # HTTP_HEADER=$(curl -m ${MAX_LIMIT_TIMEOUT} --connect-timeout ${MAX_TIMOUT} -I "${URL}${urinow}" );
        # content_length=$(echo ${HTTP_HEADER} | grep 'content-length' | cut -d' ' -f2);
        # last_modify=$(echo ${HTTP_HEADER} | grep 'last-modified' | cut -d' ' -f2);
        # etag=$(echo ${HTTP_HEADER} | grep 'etag' | cut -d' ' -f2);

        echo CSV,$(date +%s),$(echo -n "${URL}${urinow}" | sha256sum | cut -d' ' -f1),${URL}${urinow} ${FILE_DUMP_NOW}
    done

    CURRENT_TIME=$(date +%s)

    # 3. Executa a verificação
    if [ $((CURRENT_TIME - FILE_DUMP_NOW)) -ge $SECONDS_TO_WAIT ]; then
        FILE_DUMP_NOW=${date +%s}
    fi

    if [ LIST_LAST_LAYER -e '']; then
        LIST_LAST_LAYER=$list_of_urls_now
    fi
    sleep 15
done

IFS=$OLD_IFS

# 5. Verify restoration
# echo "IFS has been restored to its default value."


# cat public.curl. | grep '<a ' | grep href | cut -d' ' -f2 | cut -d'"' -f2

