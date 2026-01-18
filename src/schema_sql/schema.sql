CREATE TABLE users (
    user_id         BIGINT PRIMARY KEY,
    login           VARCHAR(100),
    avatar_url      TEXT,
    url             TEXT,
    html_url        TEXT,
    type            VARCHAR(50),
    site_admin      BOOLEAN
);
    CREATE TABLE repositories (
        repo_id     BIGINT PRIMARY KEY,
        name        VARCHAR(255),
        url         TEXT
    );
CREATE TABLE events (
    event_id        VARCHAR(32) PRIMARY KEY,
    event_type      VARCHAR(50),
    public          BOOLEAN,
    created_at      TIMESTAMP,
    actor_id        BIGINT,
    repo_id         BIGINT,
    FOREIGN KEY (actor_id) REFERENCES users(user_id),
    FOREIGN KEY (repo_id) REFERENCES repositories(repo_id)
);
CREATE TABLE issues (
    issue_id        BIGINT PRIMARY KEY,
    repo_id         BIGINT,
    number          INT,
    title           TEXT,
    state           VARCHAR(20),
    locked          BOOLEAN,
    comments_count  INT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    closed_at       TIMESTAMP,
    body            TEXT,
    user_id         BIGINT,
    FOREIGN KEY (repo_id) REFERENCES repositories(repo_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE TABLE labels (
    label_id    SERIAL PRIMARY KEY,
    name        VARCHAR(100),
    color       VARCHAR(20),
    url         TEXT
);
CREATE TABLE issue_labels (
    issue_id    BIGINT,
    label_id    BIGINT UNSIGNED,
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
CREATE TABLE payloads (
    event_id    VARCHAR(32) PRIMARY KEY,
    action      VARCHAR(50),
    FOREIGN KEY (event_id) REFERENCES events(event_id)
);
