package test.Redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class RedisClient {
	
    
    private Jedis jedis;
    private JedisPool jedisPool;
    private ShardedJedis shardedJedis;
    private ShardedJedisPool shardedJedisPool;
    
    // Configuration de la connection
    public RedisClient() 
    { 
        initialPool(); 
        initialShardedPool(); 
        shardedJedis = shardedJedisPool.getResource(); 
        jedis = jedisPool.getResource();       
    } 
   
    private void initialPool() 
    { 
        // Configuration de pool
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxTotal(20); 
        config.setMaxIdle(5); 
        config.setMaxWaitMillis(1000l); 
        config.setTestOnBorrow(false);        
        jedisPool = new JedisPool(config,"127.0.0.1",6379);
    }
    
    private void initialShardedPool() 
    { 
       
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxTotal(20); 
        config.setMaxIdle(5); 
        config.setMaxWaitMillis(1000l); 
        config.setTestOnBorrow(false); 
       
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>(); 
        shards.add(new JedisShardInfo("127.0.0.1", 6379, "master")); 
      
        shardedJedisPool = new ShardedJedisPool(config, shards); 
    } 
    
    // Gérer des numbres randoms
    public int randomNb(int min, int max) {
        Random random = new Random();
        int randomNb = random.nextInt(max)%(max-min+1) + min;
        return randomNb;
    }
    
    // Gérer des hash tables avec des pairs de key-value
    public void ajouterCalls(int nb_calls){
    	
    	List<String> status = Arrays.asList("Non affecté","Non pris en compte","En cours","Terminé");
    	List<String> text_subject = Arrays.asList("Hubert ","Tom ","Theo ","Laurent ");
    	List<String> text_verb1 = Arrays.asList("dis qu'il veut ","veut ","voudrais ","ne veut pas ");
    	List<String> text_verb2 = Arrays.asList("participer à la réunion.","demander de congé.","changer d'emploi.","être promu.");
    
    	for(int i=0;i<nb_calls;i++){
    		shardedJedis.hset("call"+String.valueOf(i), "id", String.valueOf(i)); 
            shardedJedis.hset("call"+String.valueOf(i), "hour", String.valueOf(randomNb(0,23))+":"+String.valueOf(randomNb(0,23))); 
            shardedJedis.hset("call"+String.valueOf(i), "number", "0"+String.valueOf(randomNb(100000000,999999999))); 
            shardedJedis.hset("call"+String.valueOf(i), "status", status.get(randomNb(0,3))); 
            shardedJedis.hset("call"+String.valueOf(i), "duration", String.valueOf(randomNb(20,120))+"s"); 
            shardedJedis.hset("call"+String.valueOf(i), "text", text_subject.get(randomNb(0,3))+text_verb1.get(randomNb(0,3))+text_verb2.get(randomNb(0,3))); 
            shardedJedis.hset("call"+String.valueOf(i), "operateur_id", String.valueOf(randomNb(0,3))); 
    	}
    	
    }
    
    public void ajouterOperateur(){
    	List<String> nom = Arrays.asList("Curien","Laurent","Tom","Yaya");
    	List<String> prenom = Arrays.asList("Hubert","Louis","Aldea","Frais");
    	for(int i=0; i<4; i++) {
            shardedJedis.hset("operateur"+String.valueOf(i), "id", String.valueOf(i)); 
            shardedJedis.hset("operateur"+String.valueOf(i), "nom", nom.get(i)); 
            shardedJedis.hset("operateur"+String.valueOf(i), "prenom", prenom.get(i)); 
    	}
    }

    // Simuler un call center avec Redis
    public void callCenter() {
    	
        // Suprimer tous
        jedis.flushDB(); 
        
        // Ajouter les calls
        int nb_call = randomNb(2,6);
        ajouterCalls(nb_call);
 
        // Ajouter les opérateurs
        ajouterOperateur();
        
        // Gérer les appels non traités et imprimer l'opérateur qui fait du traitement
        // Mise a jour la situation des appels non affecte
        for (int i=0;i<nb_call;i++) {
        	String status=shardedJedis.hget("call"+String.valueOf(i),"status");
        	if (status.equals("Non affecté")) {
        		System.out.println("call"+String.valueOf(i)+shardedJedis.hvals("call"+String.valueOf(i)));
        		shardedJedis.hset("call"+String.valueOf(i), "status", "En cours");
        		System.out.println("Mis a jour: ");
        		System.out.println("call"+String.valueOf(i)+shardedJedis.hvals("call"+String.valueOf(i)));
        		// Chercher l'operateur qui fait le traitement
        		String id_op=shardedJedis.hget("call"+String.valueOf(i), "operateur_id");
        		for(int j=0;j<4;j++) {
        			String id=shardedJedis.hget("operateur"+String.valueOf(j),"id");
        			if(id_op.equals(id)) {
        				System.out.println("Operateur: ");
        				System.out.println(shardedJedis.hget("operateur"+String.valueOf(j),"nom")+" "+shardedJedis.hget("operateur"+String.valueOf(j),"prenom"));
        			}
        		}
        	}
        }
    }

 }
