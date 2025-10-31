

MERGE INTO target t
USING source s
ON {merge_condition}
WHEN MATCHED THEN {matched_action}
WHEN NOT MATCHED THEN {not_matched_action}

source_table
users  email               status
peter  peter@email.com     delete
zebi   zebi@other.com      update
samarth samarth@other.com  new

target_table
users  email               status
peter  peter@email.com     current
zebi   zebi@email.com      current

  
MERGE INTO main_users_target target
USING update_users_source source
ON target.id = source.id
WHEN MATCHED AND source.status = 'update' THEN
  UPDATE SET 
    target.email = source.email,
    target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT (id, first_name, email, sign_up_date, status)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status);
