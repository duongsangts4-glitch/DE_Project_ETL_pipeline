CREATE TABLE users (
    user_id         BIGINT PRIMARY KEY,
    login           VARCHAR(100) NOT NULL,
    avatar_url      TEXT,
    url             TEXT,
    html_url        TEXT,
    type            VARCHAR(50),
    site_admin      BOOLEAN
);
CREATE TABLE repositories (
    repo_id     BIGINT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    url         TEXT
);
CREATE TABLE labels (
    label_id    SERIAL PRIMARY KEY,
    name        VARCHAR(100) UNIQUE NOT NULL,
    color       VARCHAR(20),
    url         TEXT
);
CREATE TABLE issues (
    issue_id        BIGINT PRIMARY KEY,
    repo_id         BIGINT,
    user_id         BIGINT, -- Người tạo issue
    number          INT,
    title           TEXT,
    state           VARCHAR(20),
    locked          BOOLEAN,
    comments_count  INT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    closed_at       TIMESTAMP,
    body            TEXT,
    FOREIGN KEY (repo_id) REFERENCES repositories(repo_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE TABLE issue_labels (
    issue_id    BIGINT,
    label_id    BIGINT UNSIGNED, -- Sửa lại ở đây cho đúng với SERIAL
    PRIMARY KEY (issue_id, label_id),
    FOREIGN KEY (issue_id) REFERENCES issues(issue_id),
    FOREIGN KEY (label_id) REFERENCES labels(label_id)
);
CREATE TABLE comments (
    comment_id      BIGINT PRIMARY KEY,
    issue_id        BIGINT,
    user_id         BIGINT,
    url             TEXT,
    html_url        TEXT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    body            TEXT,
    FOREIGN KEY (issue_id) REFERENCES issues(issue_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE TABLE events (
    event_id        BIGINT PRIMARY KEY, -- Chuyển từ VARCHAR sang BIGINT
    event_type      VARCHAR(50),
    public          BOOLEAN,
    created_at      TIMESTAMP,
    actor_id        BIGINT,
    repo_id         BIGINT,
    FOREIGN KEY (actor_id) REFERENCES users(user_id),
    FOREIGN KEY (repo_id) REFERENCES repositories(repo_id)
);
CREATE TABLE payloads (
    event_id    BIGINT PRIMARY KEY,
    action      VARCHAR(50),
    issue_id    BIGINT,
    comment_id  BIGINT,
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    FOREIGN KEY (issue_id) REFERENCES issues(issue_id),
    FOREIGN KEY (comment_id) REFERENCES comments(comment_id)
);