package benchmark.io;

import java.io.File;
import java.util.Calendar;

import org.apache.commons.lang.NotImplementedException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class S3Connector {

	private final String awsId;
	private final String awsSecret;
	private final String awsSessionToken;
	private final String awsRegion;
	private final String s3Url;
	private final String s3BucketName;

	public boolean upload(File dataFile) {
		String fileName = String.format("DATA_%o_%o", dataFile.length(), Calendar.getInstance().getTimeInMillis());

		try {
			BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(awsId, awsSecret, awsSessionToken);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(awsRegion)
					.withCredentials(new AWSStaticCredentialsProvider(sessionCredentials)).build();

			PutObjectRequest request = new PutObjectRequest(s3BucketName, fileName, dataFile);
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentType("plain/text");
			metadata.addUserMetadata("x-amz-meta-title", "Benchmark Upload " + fileName);
			request.setMetadata(metadata);
			s3Client.putObject(request);
			return true;
		} catch (AmazonServiceException e) {
			// transmitted, but error
			e.printStackTrace();
			return false;
		} catch (SdkClientException e) {
			// not transmitted
			e.printStackTrace();
			return false;
		}
	}

	public File download() {
		throw new NotImplementedException();
	}

	public boolean delete() {
		throw new NotImplementedException();
	}
}
