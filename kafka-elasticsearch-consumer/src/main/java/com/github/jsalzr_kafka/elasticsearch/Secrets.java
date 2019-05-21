package com.github.jsalzr_kafka.elasticsearch;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

class Secrets {
    private Document document;
    private String host;
    private String user;
    private String password;

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
        this.host = readSecret("elastic_host");
        this.user = readSecret("elastic_user");
        this.password = readSecret("elastic_password");
    }

    private String readSecret(String tagName) {
        return this.document.getElementsByTagName(tagName)
                .item(0)
                .getTextContent();
    }

    String getHost() {
        return this.host;
    }

    String getUser() {
        return this.user;
    }

    String getPassword() {
        return this.password;
    }
}
