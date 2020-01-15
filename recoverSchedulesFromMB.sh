#/bin/bash

base_url='api.mindbodyonline.com/public/v6'
site_id='977950'
api_key='strongest-api-key'

fms_host='fms-testing.c71beqgrx8rr.us-west-2.rds.amazonaws.com'
fms_user='fms_testing'
fms_db='fms_testing'

auth_host='auth-testing.c71beqgrx8rr.us-west-2.rds.amazonaws.com'
auth_user='auth_testing'
auth_db='auth_testing'

staff_user='fms_user'
staff_pass='stronkkkk-password-for-staff'

fms-exec() {
  {
cat <<EOF
$@
EOF
   } | psql -tA -h $fms_host -U $fms_user $fms_db | tr '|' '\t'
}

auth-exec() {
  {
cat <<EOF
$@
EOF
  } | psql -tA -h $auth_host -U $auth_user $auth_db | tr '|' '\t'
}

getMbId() {
  id=$1
  auth-exec "select \"mindbodyUserId\" from \"user\" where id = '$id';"
}

getMbToken() {
  curl -s -X POST \
  https://$base_url/usertoken/issue \
  -H 'Content-Type: application/json' \
  -H "Api-Key: $api_key" \
  -H "SiteId: $site_id" \
  -d "
    \"Username\": \"$staff_user\",
    \"Password\": \"$staff_pass\"
  }" | parse_token
}

start_of_week() {
  local the_day=$(date -d "$1" +%F)
  date -d "$the_day - $(date -d "$the_day" +%u)days" +%F
}

end_of_week() {
  local the_day=$(date -d "$1" +%F)
  date -d "$the_day + $((7 - $(date -d "$the_day" +%u)))days" +%F
}

parse_token() {
  jq -r '[ .TokenType,.AccessToken] | join("	")'
}

parse_visits() {
  jq -r '.Visits | .[] | [.Name,.StartDateTime,.EndDateTime] | join("	")'
}

getClientVisitsForWeek() {
  local mid=$1
  local day=$2
  local sow=$(start_of_week $day)
  local eow=$(end_of_week $day)
  
  curl -s -X GET "https://$base_url/client/clientvisits?clientId=${mid}&StartDate=${sow}&EndDate=${eow}" \
    -H "Api-Key: $api_key" \
    -H "Authorization: ${token}" \
    -H "SiteId: $site_id" | parse_visits
}

function should_skip_class() {
  classname=$1
  vertical=$2
  plan=$3

  if echo $classname | grep -q -i $vertical ; then
    [ "$vertical" == "FIT" ] && return 0

    echo $classname | grep -q -i $plan && return 0 || return 1
  fi
  return 1
}

token=$(getMbToken)
# exit if no token
[[ -z "$token" || "$token" == ' ' ]] && { echo NO token ; return 1 ; }


fms-exec "select \"athleteId\", (\"createdAt\"-'8 hours'::interval+'4week'::interval)::date, vertical, package, id \
	  from \"membershipPurchase\" where \"selectedSchedule\" is null ;" > /tmp/sin-schedule


OIFS=$IFS
IFS="	"

cat /tmp/sin-schedule | while read uid created vertical plan id ; do
  
  mid=$(getMbId $uid)

#  mid=100002250
#  created=2020-01-20
#  vertical='SPEEDLAB'
#  plan='YOUTH'

  echo procesando $uid,$mid,$vertical,$plan,$created > /dev/stderr
  
  schedule=''

  IFS="	"
  while read classname starttime endtime ; do
    should_skip_class "$classname" $vertical $plan && continue

    classname=$(echo $classname | tr ' ' '_' | tr '-' '_')

    schedule_item_raw=$(date -d "$starttime" +%a,%I:%M,%p)

    short_day_name=$(echo $schedule_item_raw | cut -f 1 -d,)

    hour_period=$(echo $schedule_item_raw | cut -f 2 -d , | cut -f 1 -d : | sed -e 's/^0//')

    min_period=$(echo $schedule_item_raw | cut -f 2 -d, | cut -f 2 -d : | sed -e 's/^0\+//')
    
    ampm=$(echo $schedule_item_raw | cut -f 3 -d,)

    [ "$min_period" ] && min_period=":$min_period"

    # EXAMPLE: [{"shortDayName":"Tue","hourWithPeriod":"5PM"},{"shortDayName":"Fri","hourWithPeriod":"5PM"}]

    schedule="${schedule}${schedule:+","}{\"shortDayName\":\"$short_day_name\",\"hourWithPeriod\":\"${hour_period}${min_period}${ampm}]\"}"

  done < <(getClientVisitsForWeek $mid "$created")

  [ $schedule ] && echo -e "$id\t$uid\t$mid\t[${schedule}]\t$vertical\t$plan"
done
