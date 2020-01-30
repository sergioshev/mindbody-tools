#!/bin/bash

usage() {
cat <<FFAA
  pecialDiff.sh -c current_csv -p previous_csv
 
  -c current_csv: the most recent csv downloaded
  -p previous_csv: the oldest one

  OUTPUT: generates 3 files

  1) current_csv.only
  2) previous_csv.only
  3) previous_csv.changed
FFAA
  exit 0
}

calculate_signature() {
  echo -e $@ | md5sum | sed -e 's/\s\+-\s*//'
}

get_fields() {
  file="$1"
  shift

  cat "$file" | cut $@ -d,
}

cleanup() {
  rm -f shared.uids shared.changed.uids "$current.uids" "$previous.uids"
}

skip=1
script_name=$0
options=$(getopt -o "c:p:h" -- "$@")
eval set -- "$options"

while true ; do
  case $1 in
    -h) usage ;;
    -p) shift ; previous="$1" ;;
    -c) shift ; current="$1" ;;
    --) shift ; break ;;
  esac
  shift
done

[[ ! $current  ]] || [[ ! $previous ]] && {
  echo Missing filenames > /dev/stderr
  usage
  exit 1
}

[[ ! -r "$current" ]] || [[ ! -r "$previous" ]] && {
  echo $current or $previous not exists > /dev/stderr
  exit 1
}

get_fields "$current" -f 1 | sort -u > "$current.uids"
get_fields "$previous" -f 1 | sort -u > "$previous.uids"

grep -F -f "$current.uids" "$previous.uids" | sed -e 's/^/^/' > shared.uids

grep -v -f shared.uids "$current" > "$current.only"
echo "$current.only" generated

grep -v -f shared.uids "$previous" > "$previous.only"
echo "$previous.only" generated

echo Searching for familyCount id changes...
{
  cat shared.uids | while read uid ; do
    current_data=$(cat "$current" | grep $uid | cut -f 1,11 -d, | sort -u)
    previous_data=$(cat "$previous" | grep $uid | cut -f 1,11 -d, | sort -u)

    current_signature=$(calculate_signature $current_data)
    previous_signature=$(calculate_signature $previous_data)

    [ ! $previous_signature == $current_signature ] && { 
      # echo $previous_data=$previous_signature --- $current_data=$current_signature --- $uid > /dev/stderr
      echo $uid 
    }
  done
} > shared.changed.uids

grep -f shared.changed.uids "$previous" > "$previous.changed"
echo "$previous.changed" generated

cleanup
