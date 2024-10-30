#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided"
echo $1 | grep -E -q '^.*-.*' && die "Please use underscores (_) instead of hyphens (-)"

for file in $(find . -type d -name '*template_python*'); do
    git mv "$file" "${file/template_python/$1}"
done

for file in $(find . -type f -name '*template_python*'); do
    git mv "$file" "${file/template_python/$1}"
done

for name in template_python Template; do
    for file in $(grep -rl $name * .github); do
        sed -i "s/$name/$1/g" $file
    done
done

hyphenated_name=$(echo $1 | sed s/_/-/g)

for file in $(grep -rl template-python *); do
    sed -i "s/template-python/$hyphenated_name/g" $file
done

echo "Template renaming complete. Please remove this file and commit your changes."