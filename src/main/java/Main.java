
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");
        
        Region region = Region.EU_SOUTH_1; // Milan
        String accessKey = "YOUR_ACCESS_KEY_ID";
        String secretKey = "YOUR_SECRET_ACCESS_KEY";
        String bucketName = "test";
        String objectKey  = "objectKey" ;
        String fileToUpload = "test.txt" ;
        
        S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey))
                .build();

        listBuckets(s3);
        
        putS3Object( s3,  bucketName,  objectKey, fileToUpload) ;
        
        s3.close();

    }
  

    public static void listBuckets(S3Client s3) {
        try {
            ListBucketsResponse response = s3.listBuckets();
            response.buckets().forEach(bucket -> {
                System.out.println("Bucket Name: " + bucket.name());
            });
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    
    public static void putS3Object(S3Client s3, String bucketName, String objectKey, String objectPath) {
     
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            s3.putObject(putOb, RequestBody.fromFile(new File(objectPath)));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

}
