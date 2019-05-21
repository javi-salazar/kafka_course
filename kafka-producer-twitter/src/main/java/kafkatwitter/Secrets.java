package kafkatwitter;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

class Secrets {
    private Document document;
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;

    Secrets() {
        File file = new File("credentials.xml");

        DocumentBuilderFactory documentBuilderFactory =
                DocumentBuilderFactory.newInstance();

        DocumentBuilder documentBuilder;

        try {
            documentBuilder = documentBuilderFactory.newDocumentBuilder();
            this.document = documentBuilder.parse(file);
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        } finally {
            this.setSecrets();
        }

    }

    private void setSecrets() {
        this.consumerKey = readSecret("consumer_key");
        this.consumerSecret = readSecret("consumer_secret");
        this.token = readSecret("token");
        this.secret = readSecret("secret");
    }

    private String readSecret(String tagName) {
        return this.document.getElementsByTagName(tagName)
                .item(0)
                .getTextContent();
    }

    String getConsumerKey() {
        return this.consumerKey;
    }

    String getConsumerSecret() {
        return this.consumerSecret;
    }

    String getToken() {
        return this.token;
    }

    String getSecret() {
        return this.secret;
    }
}
