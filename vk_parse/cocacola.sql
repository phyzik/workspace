PRAGMA foreign_keys=OFF;

BEGIN TRANSACTION;
DROP TABLE IF EXISTS messages ;
DROP TABLE IF EXISTS walls ;
DROP TABLE IF EXISTS topics ;
DROP TABLE IF EXISTS alboms ;

CREATE TABLE walls (
		id VARCHAR(32) PRIMARY KEY, -- wall-16297716_202374
		title VARCHAR(64) NOT NULL default "wall",
		url VARCHAR(64),
		begin_date VARCHAR(10),
               	last_date VARCHAR(10),
 		first_message INT,
		last_message INT,
		last_time VARCHAR(8), -- "10:32"
		offset INT,
		active INT NOT NULL default 1 
		);

CREATE TABLE topics (
                id VARCHAR(32) PRIMARY KEY,
                title VARCHAR(64) NOT NULL default "topic",
                url VARCHAR(64),
                begin_date VARCHAR(10),
                last_date VARCHAR(10),
                first_message INT,
                last_message INT,
                last_time VARCHAR(8), -- "10:32"
                offset INT,
                active INT NOT NULL default 1
               );

CREATE TABLE alboms (
                id VARCHAR(32) PRIMARY KEY,
                title VARCHAR(64) NOT NULL default "albom",
                url VARCHAR(64),
                begin_date VARCHAR(10),
                last_date VARCHAR(10),
                first_message INT,
                last_message INT,
                last_time VARCHAR(8), -- "10:32"
                offset INT,
                active INT NOT NULL default 1
               );

-- items[nn] = ( dat, topic_text, topic, author, aURL, msg, msgURL )
CREATE TABLE messages (
		id INT PRIMARY KEY,  -- post #
                    -- Date
                dat VARCHAR(10) NOT NULL default "31.05.2013",
                    -- time
                tt VARCHAR(8) NOT NULL default "00:00", 
                source VARCHAR(64) NOT NULL default "", -- point to id
                -- 
		sourcetype VARCHAR(10) not null default "topics", -- topics | walls | alboms
                    -- 
                author VARCHAR(64),
                auth_link VARCHAR(64),
                message VARCHAR(1024),
		mess_link VARCHAR(64),
		likes INT,
		data varchar(16)
             );


COMMIT;

