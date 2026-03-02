<?php
// benchmark_mysqli_write.php

$host = '127.0.0.1';
$port = 3307;
$user = 'ignis_user';
$pass = 'ignis_pass';
$db = 'ignis_db';

$mysqli = new mysqli($host, $user, $pass, $db, $port);

if ($mysqli->connect_error) {
    die("Connection failed: " . $mysqli->connect_error . "\n");
}

echo "Connected! Testing INSERT, UPDATE, DELETE...\n";

// 1. INSERT
$key_id = "test_user_" . rand(1000, 9999);
echo "\n[TEST] INSERT INTO data_users (id, name, role) VALUES ('$key_id', 'TestUser', 'Admin')\n";

if ($mysqli->query("INSERT INTO data_users (id, name, role) VALUES ('$key_id', 'TestUser', 'Admin')") === TRUE) {
    echo "✔ INSERT Successful\n";
} else {
    echo "✖ INSERT Failed: " . $mysqli->error . "\n";
}

// 2. VERIFY INSERT (SELECT)
$res = $mysqli->query("SELECT * FROM data_users WHERE id = '$key_id'");
if ($res && $row = $res->fetch_assoc()) {
    echo "✔ SELECT Verification Successful: " . json_encode($row) . "\n";
} else {
    echo "✖ SELECT Failed to find inserted key\n";
}

// 3. UPDATE
echo "\n[TEST] UPDATE data_users SET role='SuperAdmin' WHERE id='$key_id'\n";
if ($mysqli->query("UPDATE data_users SET role='SuperAdmin' WHERE id='$key_id'") === TRUE) {
    echo "✔ UPDATE Successful\n";
} else {
    echo "✖ UPDATE Failed: " . $mysqli->error . "\n";
}

// 4. VERIFY UPDATE
$res = $mysqli->query("SELECT * FROM data_users WHERE id = '$key_id'");
if ($res && $row = $res->fetch_assoc()) {
    echo "✔ SELECT (Post-Update) Verification Successful: " . json_encode($row) . "\n";
}

// 5. DELETE
echo "\n[TEST] DELETE FROM data_users WHERE id='$key_id'\n";
if ($mysqli->query("DELETE FROM data_users WHERE id='$key_id'") === TRUE) {
    echo "✔ DELETE Successful\n";
} else {
    echo "✖ DELETE Failed: " . $mysqli->error . "\n";
}

// 6. VERIFY DELETE
$res = $mysqli->query("SELECT * FROM data_users WHERE id = '$key_id'");
if ($res && $res->num_rows == 0) {
    echo "✔ DELETE Verification Successful (Row Gone)\n";
} else {
    echo "✖ Row still exists after DELETE!\n";
}

$mysqli->close();
?>