-- to pin the database use '-- database f/your/path'
-- to only return the result of the last query use '--return_last_result'
-- $1 series_id
-- $2 bodypartexamined
-- $3 tags
-- $4 authored_by
-- $5 notes
-- $6 follow-up
insert
	into
	fieldsite.validate_series
(series_id,
	bodypartexamined,
	tags,
	authored_by,
	notes,
	follow_up)
values($1::INT, $2::TEXT, $3::JSONB, $4::TEXT, $5::TEXT, $6::BOOL) RETURNING *;

