# matrix-spider-bot

_A bot that explores the Matrix federation web._

This is a Matrix bot that crawls through historical messages in rooms, looking for other Matrix room IDs. It then tries to join those rooms and continues the process.
The bot stores messages it finds in its database (including system metadata messages like edits and joins) along with the users in a room, plus various other metadata.