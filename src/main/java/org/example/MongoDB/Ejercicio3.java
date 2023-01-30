package org.example.MongoDB;

import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.example.Colores;

import java.util.ArrayList;
import java.util.List;

public class Ejercicio3 {

    private static MongoCollection<Document> col;

    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb+srv://pnav:17072003@cluster0.8awmtte.mongodb.net/?retryWrites=true&w=majority");
        MongoDatabase db = client.getDatabase("BDPeliculas");
        col = db.getCollection("peliculas");

        ejercicioUno();

        ejercicioDos();

        ejercicioTres();

        ejercicioCuatro();

        ejercicioCinco();

        ejercicioSeis();

        ejercicioSiete();

        ejercicioOcho();
    }

    private static void imprimirTitulo(String titulo) {
        imprimirSeparador(titulo);
        Colores.imprimirAzul(titulo);
        imprimirSeparador(titulo);
    }

    private static void imprimirSeparador(String titulo) {
        for (int i = 0; i < titulo.length(); i++) {
            System.out.print(Colores.ANSI_CYAN + "-" + Colores.ANSI_RESET);
        }
        System.out.println();
    }

    public static void ejercicioUno() {
        imprimirTitulo("Ejercicio uno");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("storyline", new BasicDBObject("$regex", "crime").append("$options", "i"));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioDos() {
        imprimirTitulo("Ejercicio dos");

        BasicDBObject consulta = new BasicDBObject();

        consulta.put("genres", "Drama");

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres",String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
//            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class));
        }
        cursor.close();
    }

    public static void ejercicioTres() {
        imprimirTitulo("Ejercicio tres");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();
        dbObjects.add(new BasicDBObject("genres", "Drama"));
        dbObjects.add(new BasicDBObject("actors.starring", "Brad Pitt"));

        consulta.put("$and", dbObjects);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioCuatro() {
        imprimirTitulo("Ejercicio cuatro");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();
        dbObjects.add(new BasicDBObject("actors.starring", "Morgan Freeman"));
        dbObjects.add(new BasicDBObject("actors.cast", "Morgan Freeman"));

        consulta.put("$or", dbObjects);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class) + "\tDe reparto --> " + actores.getList("cast",String.class));
        }
        cursor.close();
    }

    public static void ejercicioCinco() {
        imprimirTitulo("Ejercicio cinco");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();
        dbObjects.add(new BasicDBObject("actors.starring", "Charles Chaplin"));

        List<String> actoresLista = new ArrayList<>();
        actoresLista.add("Paulette Goddard");
        actoresLista.add("Florence Lee");
        dbObjects.add(new BasicDBObject("actors.cast", new BasicDBObject("$all", actoresLista)));

        consulta.put("$and", dbObjects);

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioSeis() {
        imprimirTitulo("Ejercicio seis");

        BasicDBObject consulta = new BasicDBObject();


        consulta.put("actors.starring",new BasicDBObject("$size",3));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioSiete() {
        imprimirTitulo("Ejercicio siete");

        BasicDBObject consulta = new BasicDBObject();


        consulta.put("year",new BasicDBObject("$gt","2010"));

        FindIterable<Document> resultDocument = col.find(consulta);

        MongoCursor<Document> cursor = resultDocument.cursor();
        while (cursor.hasNext()) {
            Document documento = cursor.next();
            Document actores = (Document) documento.get("actors");
            System.out.println(documento.get("title") + " " + documento.get("year") + "\tGéneros --> " + documento.getList("genres", String.class) + "\tProtagonistas --> " + actores.getList("starring", String.class));
        }
        cursor.close();
    }

    public static void ejercicioOcho() {
        imprimirTitulo("Ejercicio ocho (Películas que tengan solo un protagonista, que sea Christian Bale o Charles Chaplin y producidas antes del 2005 o en el 2005");

        BasicDBObject consulta = new BasicDBObject();

        List<BasicDBObject> dbObjects = new ArrayList<>();

        dbObjects.add(new BasicDBObject("actors.starring",new BasicDBObject("$size",1)));

        List<String> actoresLista = new ArrayList<>();
        actoresLista.add("Christian Bale");
        actoresLista.add("Charles Chaplin");

        dbObjects.add(new BasicDBObject("actors.starring",new BasicDBObject("$in",actoresLista)));
        dbObjects.add(new BasicDBObject("year",new BasicDBObject("$lte","2005")));

        consulta.put("$and",dbObjects);

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