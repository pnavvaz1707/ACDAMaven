package org.example.MongoDB;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class C {
    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb+srv://pnav:17072003@cluster0.8awmtte.mongodb.net/?retryWrites=true&w=majority");
        MongoDatabase db = client.getDatabase("BDPeliculas");
        MongoCollection<org.bson.Document> collection = db.getCollection("peliculas");

        consultaSimple2(collection);

        consultaListaSimple(collection);

        consultaListaTamano(collection);

        consultaObjetoJson(collection);

        consultaObjetoJson2(collection);

        consultaOperadores(collection);

        consultaLike(collection);
    }

    private static void imprimirTitulo(String titulo){
        for (int i = 0; i < titulo.length(); i++) {
            System.out.print("-");
        }
        System.out.println();
        System.out.println(titulo);
        for (int i = 0; i < titulo.length(); i++) {
            System.out.print("-");
        }
        System.out.println();
    }

    private static void consultaSimple2(MongoCollection col) {
        imprimirTitulo("Consulta sobre todas las películas del año 1994 y publicadas con el título original");

        BasicDBObject consulta = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<>();
        obj.add(new BasicDBObject("year", "1994"));
        obj.add(new BasicDBObject("originalTitle", ""));
        consulta.put("$or", obj);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            System.out.println(documento.get("title") + " " + documento.get("year"));
        }
        cursor.close();
    }

    private static void consultaListaSimple(MongoCollection col) {
        imprimirTitulo("Consulta sobre todas las películas con el género 'Crime' y 'Drama'");

        BasicDBObject consulta = new BasicDBObject();
        List<String> list = new ArrayList<>();
        list.add("Crime");
        list.add("Drama");
        consulta.put("genres", new BasicDBObject("$all", list));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            System.out.println(documento.get("title") + " " + documento.get("year") + documento.getList("genres", String.class));
        }
        cursor.close();
    }

    private static void consultaListaTamano(MongoCollection col) {
        imprimirTitulo("Consulta sobre todas las películas catalogadas con dos géneros cinematográficos");

        BasicDBObject consulta = new BasicDBObject();
        consulta.put("genres", new BasicDBObject("$size", 2));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            System.out.println(documento.get("title") + " " + documento.get("year") + documento.getList("genres", String.class));
        }
        cursor.close();
    }

    private static void consultaObjetoJson(MongoCollection col) {
        imprimirTitulo("Consulta sobre todas las películas protagonizadas por Tim Robbins o Brad Pitt");

        BasicDBObject consulta = new BasicDBObject();
        List<String> list = new ArrayList<>();
        list.add("Tim Robbins");
        list.add("Brad Pitt");
        consulta.put("actors.starring", new BasicDBObject("$in", list));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    private static void consultaObjetoJson2(MongoCollection col) {
        imprimirTitulo("Consulta sobre todas las películas protagonizadas por Tim Robbins o Brad Pitt y de género 'Crime'");

        BasicDBObject consulta = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<>();

        obj.add(new BasicDBObject("genres", "Crime"));

        List<String> list = new ArrayList<>();
        list.add("Tim Robbins");
        list.add("Brad Pitt");

        obj.add(new BasicDBObject("actors.starring", new BasicDBObject("$in", list)));

        consulta.put("$and", obj);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    private static void consultaOperadores(MongoCollection col) {
        imprimirTitulo("Consulta de las películas cuyo año sea mayor que 2000 y menores que 2010");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("year", new BasicDBObject("$gt", "2000").append("$lt", "2010"));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    private static void consultaLike(MongoCollection col) {
        imprimirTitulo("Consulta Like");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("originalTitle", new BasicDBObject("$regex","ing$").append("$options","i"));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }
}
