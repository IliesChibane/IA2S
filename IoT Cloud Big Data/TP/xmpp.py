import slixmpp

# Create a custom XMPP client class
class MyXMPPClient(slixmpp.ClientXMPP):
    def __init__(self, jid, password):
        slixmpp.ClientXMPP.__init__(self, jid, password)

        # Register event handlers
        self.add_event_handler("session_start", self.start)

    def start(self, event):
        self.send_presence()
        self.get_roster()

        # Send the image file
        self.send_file(
            jid='recipient@example.com',
            file='path/to/image.jpg',
            name='image.jpg',
            size=12345
        )

# Create an instance of the custom XMPP client
xmpp = MyXMPPClient('your_jid@example.com', 'your_password')

# Connect to the XMPP server and start processing events
xmpp.connect()
xmpp.process(forever=False)
