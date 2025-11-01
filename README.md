# FileStreamBot

## A Telegram Bot to Convert Telegram Files To Direct & Streamable Links.

### About

This bot provides stream links for Telegram files without the necessity of waiting for the download to complete, offering the ability to store files.

### Features

-   Stream Telegram files instantly.
-   No need to wait for the download to complete.
-   Supports multiple MongoDB databases for file storage.
-   Referral system to earn premium access.
-   Admin panel with a lot of features.

### How to Deploy

You can deploy this bot on any platform that supports Python. Here are the instructions for some popular platforms:

**Deploy on Heroku:**

1.  Fork this repository.
2.  Click on the "Deploy to Heroku" button below.
3.  Fill in the required environment variables.
4.  Click on "Deploy app".

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy)

**Deploy Locally:**

1.  Clone this repository:
    ```sh
    git clone https://github.com/avipatilpro/FileStreamBot
    cd FileStreamBot
    ```
2.  Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```
3.  Set the required environment variables in a `.env` file.
4.  Run the bot:
    ```sh
    python3 -m FileStream
    ```

### Commands

**User Commands:**

-   `/start` - Start the bot.
-   `/dl` - Get a direct download link for a file.
-   `/help` - Show the help message.
-   `/info` - Get bot information.
-   `/refer` - Get your referral link to earn premium access.
-   `/request <name>` - Request a file.
-   `/request_index` - Request a file or channel to be indexed.
-   Send any text to search for a file.

**Admin Commands:**

-   `/log` - Show recent error logs.
-   `/total_users` - Get the total number of users.
-   `/total_files` - Get the total number of files in the current DB.
-   `/stats` - Get bot and database statistics.
-   `/findfile <name>` - Find a file's ID by name.
-   `/recent` - Show the 10 most recently uploaded files.
-   `/deletefile <id>` - Delete a file from the database.
-   `/deleteall` - Delete all files from the current database.
-   `/ban <user_id>` - Ban a user.
-   `/unban <user_id>` - Unban a user.
-   `/freeforall` - Grant 12-hour premium access to all users.
-   `/broadcast <msg>` - Send a message to all users.
-   `/grp_broadcast <msg>` - Send a message to all connected groups where the bot is an admin.
-   `/index_channel <channel_id> [skip]` - Index files from a channel.
-   Send a file to me in a private message to index it.

### Environment Variables

| Variable            | Description                                                     | Example                                                                                                  |
| ------------------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `BOT_TOKEN`         | Your bot's token from @BotFather.                               | `1234567890:ABC-DEF1234ghIkl-zyx57W2v1u123ew11`                                                          |
| `DB_CHANNEL`        | The ID of the channel where the bot will store files.           | `-1001234567890`                                                                                         |
| `LOG_CHANNEL`       | The ID of the channel where the bot will send logs.             | `-1001234567890`                                                                                         |
| `JOIN_CHECK_CHANNEL`| The IDs of the channels that users must join to use the bot.   | `-1001234567890 -1009876543210`                                                                          |
| `ADMINS`            | The user IDs of the bot's admins.                               | `123456789 987654321`                                                                                    |
| `PM_SEARCH_ENABLED` | Set to `True` to allow non-admins to search for files in PM.    | `True`                                                                                                   |
| `MONGO_URIS`        | The MongoDB URIs for the file databases.                        | `mongodb+srv://... mongodb+srv://...`                                                                    |
| `GROUPS_DB_URIS`    | The MongoDB URIs for the group databases.                       | `mongodb+srv://... mongodb+srv://...`                                                                    |
| `REFERRAL_DB_URI`   | The MongoDB URI for the referral database.                      | `mongodb+srv://...`                                                                                      |
| `PORT`              | The port that you want your webapp to be listened to.           | `8080`                                                                                                   |

### Contributing

Contributions are welcome! If you have any ideas, suggestions, or bug reports, please open an issue or submit a pull request.

### License

This project is licensed under the MIT License.
