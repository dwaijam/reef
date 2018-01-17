using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.AzureDatLake
{
    public sealed class AzureDataLakeFileSystemConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Set AzureDataLakeFileSystemConfigurationProvider to DriverConfigurationProviders.
        /// Set all the parameters needed for injecting AzureDataLakeFileSystemConfigurationProvider.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new AzureDataLakeFileSystemConfiguration()
            .BindSetEntry<DriverConfigurationProviders, AzureDataLakeFileSystemConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<AzureDataLakeFileSystemConfigurationProvider>.Class)
            .BindImplementation(GenericType<IFileSystem>.Class, GenericType<AzureDataLakeFileSystem>.Class)
            .Build();
    }
}
