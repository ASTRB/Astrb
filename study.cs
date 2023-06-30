using IrobotBox.Core.Extension.Core.System.Guid;

using IrobotBox.Erp.Api.Sdk.ResponseModel;

using IrobotBox.Market.Sdk.Mercadolibre.Model;

using IrobotBox.Market.Sdk.Mercadolibre.RequestModel;

using IrobotBox.MarketPublish.JobLibrary.Implement.CalculatorClientService.RequestModel;

using IrobotBox.MarketPublish.JobLibrary.Implement.CalculatorClientService.ResponseModel;

using IrobotBox.MarketPublish.JobLibrary.Implement.CommonService;

using IrobotBox.MarketPublish.JobLibrary.Interface.ClientService;

using IrobotBox.MarketPublish.JobLibrary.Interface.CommonService;

using IrobotBox.MarketPublish.JobLibrary.Interface.JobService;

using IrobotBox.MarketPublish.Mongo.Interface;

using IrobotBox.MarketPublish.Mongo.Model;

using IrobotBox.MarketPublish.Redis.Interface;

using IrobotBox.MarketPublish.ViewModels.AutoPublish;

using IrobotBox.MarketPublish.ViewModels.AutoPublishStrategy;

using IrobotBox.MarketPublish.ViewModels.Common;

using IrobotBox.MarketPublish.ViewModels.Marketplace;

using IrobotBox.MarketPublish.ViewModels.Product;

using IrobotBox.MarketPublish.ViewModels.Product.ProductInformation;

using IrobotBox.MarketPublish.ViewModels.Systems;

using IrobotBox.RabbitMQ.Core;

using IrobotBox.ViewModel.Core.Common;

using Microsoft.Extensions.Logging;

using Newtonsoft.Json;

using System;

using System.Collections.Generic;

using System.Linq;

using System.Text;

using System.Text.RegularExpressions;

using System.Threading;

using IrobotBox.MarketPublish.JobLibrary.Extensions;

using IrobotBox.MarketPublish.Redis.Model;

using IrobotBox.MarketPublish.Standard.AutoPublish;

using IrobotBox.RabbitMQ.Core.Model;

using IrobotBox.MarketPublish.Models.Marketplace;



namespace IrobotBox.MarketPublish.JobLibrary.Implement.JobService.ExtraBaseJobService

{

    public class MercadolibreBaseJobService : HandleService

    {

        #region 参数定义

        private readonly IBaseApiService _baseApiService;

        private readonly IContentClientService _contentClientService;

        private readonly IAutoPublishClientService _autoPublishClientService;

        private readonly IMercadolibrePublishDetailRepository _mercadolibreMongo;

        private readonly IPublishConsumer _publishConsumer;

        public readonly string ErrorInfoKey = "Mercadolibre_{0}";

        private readonly MessagePublishService _messagePublishService;

        private readonly IMercadolibreListingClientService _mercadolibreListingClientService;

        private readonly IMarketClientService _marketClientService;

        #endregion



       // private readonly List<int> _newCategoryOrderSourceIds = new List<int>

      //  {

       //     11895, 11912, 16066//Mercadolibre

       // };



        public MercadolibreBaseJobService(

            IBaseApiService baseApiService,

            IContentClientService contentClientService,

            IAutoPublishClientService autoPublishClientService,

            IMercadolibrePublishDetailRepository mercadolibreMongo,

            JobUseConfig jobUseConfig,

            IPublishConsumer publishConsumer,

            MessagePublishService messagePublishService,

            IMercadolibreListingClientService mercadolibreListingClientService,

            IMarketClientService marketClientService,

            ILogger logger) : base(jobUseConfig, logger)

        {

            _baseApiService = baseApiService;

            _contentClientService = contentClientService;

            _autoPublishClientService = autoPublishClientService;

            _mercadolibreMongo = mercadolibreMongo;

            _publishConsumer = publishConsumer;

            _messagePublishService = messagePublishService;

            _mercadolibreListingClientService = mercadolibreListingClientService;

            _marketClientService = marketClientService;

        }



        #region 刊登模型预创建



        protected ProductRequestModel CreateProductModel(Ap_AutoPublish_Strategy_Io_ViewModel strategyView,

            P_Product_Io_ViewModel productView,

            Cm_Market_Product_AutoPublish_ViewModel productAutoPublishView,

            //List<ProductEditResponseModel> multiDatasViews,

            List<Sys_Market_Site_Io_ViewModel> marketSites,

            Cm_Market_ProductImage_Table_ViewModel marketProductImage,

            IEnumerable<KeyValuePair<string, string>> keyWords = null,

            Dictionary<string, Tuple<string, string>> contents = null)

        {

            //公共

            var sku = productView.Sku;

            var customerId = productView.CustomerId;

            var view = productAutoPublishView.View;

            var publishToSiteCodes = strategyView.PublishToSiteCodes;

            var attributeViews = productAutoPublishView.AttributeViews;

            var variantAttributeDefinitionViews = productAutoPublishView.VariantAttributeDefinitionViews;

            var variantViews = productAutoPublishView.VariantViews;

            var mercadolibreAttributes = productAutoPublishView.MercadolibreAttributeViews;

            //生成站点语言

            var marketSite = marketSites.FirstOrDefault(m => m.Code == view.SiteCode);

            var language = marketSite?.Language;

            //图片

            var imageViews = productAutoPublishView.VariantProductImageViews;

            //过滤禁用图片

            if (marketProductImage != null && marketProductImage.Images.Any())

            {

                List<string> urls = marketProductImage.Images.Select(s => s.Url).ToList();

                imageViews = imageViews.Where(w => !urls.Contains(w.Url)).ToList();

            }

            //主图

            var mainSkuImageViews = imageViews.Where(m => m.Sku == productView.Sku).ToList();

            var mainImageView = _baseApiService.GetRandomWhiteBackgroundImages(new List<int>(), 1, mainSkuImageViews);

            //附图

            var extraSkuImageViews = imageViews.Where(m => m.Sku == productView.Sku).ToList();

            var extraImageView = _baseApiService.GetRandomImages(false, mainImageView.Ids, 7, extraSkuImageViews);

            //全部图片

            var images = new List<PictureRequestModel>();

            mainImageView.Urls.ForEach(m => images.Add(new PictureRequestModel { Source = m }));

            extraImageView.Urls.ForEach(m => images.Add(new PictureRequestModel { Source = m }));

            //过滤掉没有主图的listing

            if (string.IsNullOrEmpty(mainImageView.Urls.FirstOrDefault()))

            {

                images.Clear();

            }



            //变体属性自定义名称

            var definitionViews = variantAttributeDefinitionViews

                .Where(m => m.ProductId == view.ProductId).ToList();



            var content = string.Empty;

            if (contents.ContainsKey(sku))

            {

                content = contents[sku].Item1!;

            }

            else if (contents.ContainsKey(view.Sku))

            {

                content = contents[view.Sku].Item1!;

            }

            var defaultAttributeValueName = "as the picture";

            var match = Regex.Match(content, "Material:(?<p>.*?)\\.<br");

            defaultAttributeValueName = match.Groups.Count >= 2 ? match.Groups[1].Value.Trim().Split(",")[0] : defaultAttributeValueName;

            var needUpdateFullAttributes = new List<string> { "CHILD_DEPENDENT", "CHILD_PK", "FAMILY", "PARENT_PK" };

            var keyword = keyWords?.FirstOrDefault(m => m.Key == productView.Sku);

            var attributes = new List<ProductAttributeModel>();

            mercadolibreAttributes!.ForEach(m =>

            {

                var productAttribute = new ProductAttributeModel();

                if (needUpdateFullAttributes.Contains(m!.View?.Hierarchy))

                {

                    var originalAttributeName = m.View?.AttributeId;

                    var attributeName = originalAttributeName?.ToUpper();



                    //属性名凡是包含color（不区分大小写），均默认填写产品库颜色

                    if (attributeName!.Contains("COLOR"))

                    {

                        productAttribute.Id = attributeName;

                        productAttribute.ValueName = productView.Color;

                    }



                    //属性名凡是包含size（不区分大小写），均默认填写产品库尺寸

                    if (attributeName!.Contains("SIZE"))

                    {

                        productAttribute.Id = attributeName;

                        productAttribute.ValueName = productView.Size;

                    }



                    //属性名凡是包含material（不区分大小写），均默认填写报关材质

                    if (attributeName!.Contains("MATERIAL"))

                    {

                        productAttribute.Id = attributeName;

                        productAttribute.ValueName = defaultAttributeValueName;

                    }



                    var item = m.ValueViews?.FirstOrDefault();

                    var noOptions = m.ValueViews?.FirstOrDefault(n => n!.Name.Equals("No"));

                    var words = keyword?.Value?.Split(",");

                    switch (attributeName)

                    {

                        //设置品牌，默认值：unbrand

                        case "BRAND":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = "unbrand";

                            break;

                        //设置材质，默认产品资料报关材质

                        case "MATERIAL":

                            //临时处理为默认值

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = defaultAttributeValueName;

                            break;

                        //设置颜色，默认产品资料颜色

                        case "COLOR":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = productView.Color.IsNotNullOrEmpty() ? productView.Color : "As Shown";

                            break;

                        //设置规格尺寸，默认产品资料规格尺寸

                        case "SIZE":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = productView.Size.IsNotNullOrEmpty() ? productView.Size : "As Shown";

                            break;

                        //设置规格长度，默认产品资料规格长度

                        case "LENGTH":

                            if (productView.Length != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Length:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置规格宽度，默认产品资料规格宽度

                        case "WIDTH":

                            if (productView.Width != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Width:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置规格高度，默认产品资料规格高度

                        case "HEIGHT":

                            if (productView.Height != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Height:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置毛重，默认产品资料毛重

                        case "WEIGHT":

                            if (productView.GrossWeight != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{(productView.GrossWeight / 1000):0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包装长度，默认产品资料规格长度

                        case "TOTAL_LENGTH":

                            if (productView.Length != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Length:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包装宽度，默认产品资料规格宽度

                        case "TOTAL_WIDTH":

                            if (productView.Width != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Width:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包装高度，默认产品资料规格高度

                        case "TOTAL_HEIGHT":

                            if (productView.Height != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Height:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包装重量，默认产品资料毛重

                        case "UNIT_WEIGHT":

                            if (productView.GrossWeight != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.GrossWeight:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包装净重，默认产品资料毛重

                        case "NET_WEIGHT":

                            if (productView.GrossWeight != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.GrossWeight:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包裹长度，默认产品资料包裹长度

                        case "PACKAGE_LENGTH":

                            if (productView.PackageLength != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.PackageLength:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包裹宽度，默认产品资料包裹宽度

                        case "PACKAGE_WIDTH":

                            if (productView.PackageWidth != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.PackageWidth:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包裹高度，默认产品资料包裹高度

                        case "PACKAGE_HEIGHT":

                            if (productView.PackageHeight != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.PackageHeight:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置包裹重量，默认产品资料包裹重量

                        case "PACKAGE_WEIGHT":

                            if (productView.PackageWeight != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.PackageWeight:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置商品 STYLES，默认产品资料第一个搜索关键词

                        case "STYLES":

                            if (words != null && words.Any())

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = words![0];

                            }

                            break;

                        //设置商品 TYPE，默认产品资料第一个搜索关键词

                        case "TYPE":

                            if (words != null && words.Any())

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = words![0];

                            }

                            break;

                        //设置商品 MODEL，默认产品资料第一个搜索关键词

                        case "MODEL":

                            if (words != null && words.Any())

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = words![0];

                            }

                            break;

                        //设置商品用途，默认产品资料用途

                        case "RECOMMENDED_USES":

                            //TODO 临时不处理

                            //productAttribute.ValueName = keyword.Value.Value;

                            break;

                        //设置商品面料做工，默认产品资料前两个搜索关键词

                        case "FABRIC_DESIGN":

                            if (keyword?.Value != null)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = keyword.Value.Value;

                            }

                            break;

                        //设置商品面料做工，默认产品资料包装清单

                        case "ACCESSORIES_INCLUDED":

                            //TODO 临时不处理

                            //productAttribute.ValueName = keyword.Value.Value;

                            break;

                        //设置包装单位，默认产品资料包装清单第一个量词

                        case "UNITS_PER_PACKAGE":

                            //TODO 临时不处理

                            //productAttribute.ValueName = keyword.Value.Value;

                            break;

                        //设置包装单位，默认产品资料包装清单第一个量词

                        case "UNITS_PER_PACK":

                            //TODO 临时不处理

                            //productAttribute.ValueName = keyword.Value.Value;

                            break;

                        //设置包装直径，默认产品资料规格宽度

                        case "DIAMETER":

                            if (productView.Width != default)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = $"{productView.Width:0.##} {m.View.DefaultUnit}";

                            }

                            break;

                        //设置商品来源地，默认 China

                        case "ORIGIN":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = "China";

                            break;

                        //设置产地，默认 China

                        case "MANUFACTURER":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = "China";

                            break;

                        //设置最小适龄，默认 5

                        case "MIN_RECOMMENDED_AGE":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = $"5 {m.View.DefaultUnit}";

                            break;

                        //设置最大适龄，默认 65

                        case "MAX_RECOMMENDED_AGE":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = $"65 {m.View.DefaultUnit}";

                            break;

                        //设置产品外形，默认 As Shown

                        case "SHAPE":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = "As Shown";

                            break;

                        //设置人工标题，默认与产品标题一致

                        case "MANUAL_TITLE":

                            if (productView.NameEn.IsNotNullOrEmpty())

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = productView.NameEn;

                            }

                            break;

                        //设置包装类型，默认 unit

                        case "PACKAGING_TYPE":

                            productAttribute.Id = attributeName;

                            productAttribute.ValueName = "unit";

                            break;

                        //设置产品适用性别

                        case "GENDER":

                            var defaultValue = m.ValueViews!.FirstOrDefault(n => n!.Name.Equals("Gender neutral"));

                            if (defaultValue != null)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = defaultValue.Name;

                                productAttribute.ValueId = defaultValue.ValueId;

                            }

                            break;

                        //设置产品是否具有为易燃性，默认选择 No

                        case "IS_FLAMMABLE":

                            if (noOptions != null)

                            {

                                productAttribute.Id = attributeName;

                                productAttribute.ValueName = noOptions.Name;

                                productAttribute.ValueId = noOptions.ValueId;

                            }

                            break;

                        default:

                            var valueType = m.View!.ValueType;

                            productAttribute.Id = attributeName;

                            switch (valueType)

                            {

                                //当属性值类型 LIST 时，选择多项值，自动处理为 第一项

                                case "list":

                                    productAttribute.ValueName = item?.Name;

                                    productAttribute.ValueId = item?.ValueId;

                                    break;

                                //当选择为 输入值 时，自动处理为 1

                                case "string":

                                case "number":

                                    productAttribute.ValueName =

                                        productAttribute.ValueName.IsNullOrEmpty() && valueType.Equals("string")

                                            ? "As Shown"

                                            : "1";

                                    break;

                                //当选择为 输入值 时，自动处理为 1 单位

                                case "number_unit":

                                    productAttribute.ValueName = $"1 {m.View.DefaultUnit}";

                                    break;

                                //当属性值类型 BOOLEAN 时，选择 Yes/No，自动处理为 No

                                case "boolean":

                                    productAttribute.ValueName = noOptions?.Name;

                                    productAttribute.ValueId = noOptions?.ValueId;

                                    break;

                                //不处理此属性值类型

                                case "picture_id":

                                    break;

                            }

                            break;

                    }

                }

                else

                {

                    var filterAttributes = new List<string> { "BRAND", "MODEL" };

                    var isAttributeHasFiltering = !filterAttributes.Contains(m.View!.AttributeId?.ToUpper());

                    //必填属性，排除 BRAND MODEL

                    if ((m.View!.Required || m.View.CatalogRequired) && isAttributeHasFiltering)

                    {

                        var attributeValue = m!.ValueViews?.FirstOrDefault(n => n!.AttributeId == m.View.AttributeId);

                        productAttribute.Id = m.View.AttributeId;

                        productAttribute.ValueName = attributeValue?.Name ?? "as the picture";

                        switch (m.View.ValueType)

                        {

                            case "string":

                                break;

                            case "number_unit":

                                productAttribute.ValueName = $"1 {m.View.DefaultUnit}";

                                break;

                            case "number":

                                productAttribute.ValueName = "1";

                                break;

                            case "list":

                                productAttribute.ValueId = attributeValue!.ValueId;

                                break;

                            case "boolean":

                                productAttribute.ValueId = attributeValue!.ValueId;

                                break;

                        }

                    }

                }

                attributes.Add(productAttribute);

            });



            //设置品牌属性

            if (attributes.All(m => m!.Id != "BRAND"))

            {

                var brand = _baseApiService.GetBrand(strategyView, "unbrand", productView.BrandName);

                attributes.Add(new ProductAttributeModel

                {

                    Id = "Brand",

                    ValueName = brand.IsNullOrEmpty() ? "unbrand" : brand

                });

            }

            //设置 GTIN /UPC /EAN

            if (attributes.All(m => m!.Id != "GTIN"))

            {

                attributes.Add(new ProductAttributeModel

                {

                    Id = "GTIN",

                    ValueName = string.Empty

                });

            }

            //设置产品型号

            if (attributes.All(m => m!.Id != "MODEL"))

            {

                if (view!.Tags.IsNotNullOrWhiteSpace())

                {

                    var valueName = string.Empty;

                    var tags = JsonConvert.DeserializeObject<List<string>>(view.Tags!);

                    if (tags != null && tags.Any())

                    {

                        valueName = tags.FirstOrDefault();

                    }

                    attributes.Add(new ProductAttributeModel

                    {

                        Id = "Model",

                        ValueName = valueName

                    });

                }

                else

                {

                    attributes.Add(new ProductAttributeModel

                    {

                        Id = "Model",

                        ValueName = "as the picture"

                    });

                }

            }



            //变体

            var subViews = variantViews.GroupBy(m =>

                    new { m.CustomerId, m.ProductId, m.MarketId, m.Sku, m.CustomSku })

                .Select(m => m.Key)

                .ToList();

            //SKU列表

            var colorAttributes = new List<string>();



            var skuIndex = 1;

            var skuViews = subViews.Select(sv =>

            {

                var variantAttributeViews = variantViews.Where(m =>

                    m.Sku.Equals(sv.Sku)).ToList();



                //图片

                var skuImages = imageViews.Where(m => m.Sku == sv.Sku).ToList();

                var mainImageView = _baseApiService.GetRandomWhiteBackgroundImages(new List<int>(), 1, skuImages);

                var images = new List<string>();

                images.AddRange(mainImageView.Urls);

                //附图

                var extraImageView = _baseApiService.GetRandomImages(false, mainImageView.Ids, 7, skuImages);

                images.AddRange(extraImageView.Urls);



                if (!images.Any())

                {

                    images = imageViews.Take(8).Select(m => m.Url).ToList();

                }

                //过滤掉没有主图的listing

                if (string.IsNullOrEmpty(mainImageView.Urls.FirstOrDefault()))

                {

                    images.Clear();

                }



                #region 颜色

                var colorAttribute = variantAttributeViews.FirstOrDefault(a => a.AttributeName.Contains("color", StringComparison.OrdinalIgnoreCase));



                var valueName = "as the picture";

                if (colorAttribute != null && !string.IsNullOrEmpty(colorAttribute.AttributeValue))

                {

                    valueName = colorAttribute.AttributeValue;

                }

                //有重复的特殊处理

                if (colorAttributes.Contains(valueName))

                {

                    valueName = $"{valueName}{skuIndex}";

                }

                colorAttributes.Add(valueName);

                #endregion



                #region 处理多变体

                var attributeCombinations = new List<ProductAttributeModel>();

                if (subViews.Count > 1)

                {

                    var mercadolibreVariationAttributes = mercadolibreAttributes.Where(m => m.View.AllowVariations).ToList();

                    if (mercadolibreVariationAttributes.Any())

                    {

                        if (mercadolibreVariationAttributes.Any(m => m.View.AttributeId.Contains("color", StringComparison.OrdinalIgnoreCase)))

                        {

                            var colorAttributeModel = new ProductAttributeModel

                            {

                                Id = "COLOR",

                                ValueName = valueName

                            };

                            attributeCombinations.Add(colorAttributeModel);

                        }

                    }

                }



                #endregion



                skuIndex++;



                var variantAttributeList = new List<ProductAttributeModel>();

                //变体自定义属性

                variantAttributeViews.ForEach(v =>

                {

                    if (!v.MarketVariantAttributeId.HasValue && !v.MarketVariantAttributeId.HasValue)

                    {

                        return;

                    }

                    if (definitionViews.Exists(m =>

                            m.MarketVariantAttributeId == v.MarketVariantAttributeId &&

                            m.MarketVariantValueId == v.MarketVariantValueId))

                    {

                        return;

                    }



                    variantAttributeList.Add(new ProductAttributeModel

                    {

                        Id = v.AttributeId.ToString(),

                        ValueName = v.AttributeValue

                    });

                });



                return new ProductVariationRequestModel

                {

                    Attributes = GetProductAtrributes(productView),

                    PictureIds = images,

                    SellerSku = sv.Sku,

                    AttributeCombinations = attributeCombinations

                };

            }).ToList();

            colorAttributes.ForEach(x =>

            {

                var colorAttributeModel = new ProductAttributeModel

                {

                    Id = "COLOR",

                    Name = "Color",

                    ValueName = x

                };

                attributes.Add(colorAttributeModel);//颜色属性

            });

            //产品质量保证

            var sellerItems = new List<SaleTermRequestModel> { new SaleTermRequestModel

            {

                Id="WARRANTY_TYPE",

                ValueId = "6150835",

                ValueName = "No warranty"

            }};



            //构建属性

            return new ProductRequestModel

            {

                ListingTypeId = "gold_pro",

                Title = view.Title,//Left(view.Title, 60), //标题 来源于ERP标题库

                CategoryId = view.CategoryId,

                BuyingMode = "buy_it_now", //购买方式

                SiteId = "CBT", //站点

                CurrencyId = "USD",

                Condition = "new",

                Attributes = attributes.Where(a => !string.IsNullOrEmpty(a.Id)).ToList(),

                Pictures = images,

                Variations = skuViews,

                SaleTerms = sellerItems

            };

        }



        #endregion



        #region 刊登到指定渠道



        /// <summary>

        /// 刊登到指定渠道

        /// </summary>

        /// <param name="dataView"></param>

        /// <param name="autoPublishStrategy"></param>

        /// <param name="currentPublishesViews"></param>

        /// <param name="views"></param>

        /// <param name="inventoryModels"></param>

        /// <param name="products"></param>

        /// <param name="situationViews"></param>

        /// <returns></returns>

        protected ReturnedView AutoPublish(

            Cm_AutoPublish_Strategy_ViewModel autoPublishStrategy,

            List<Cm_Product_AutoPublish_ViewModel> views,

            List<InventoryResponseModel> inventoryModels,

            List<ProductRequestModel> products,

            List<M_Custom_Sensitive_Keywords_Io_ViewModel> customSensitiveKeywordsViews,

            LogContentView loggerData,

            bool isVariation,

            CancellationToken jobToken,

            ref List<int> limitOrderSources,

            ref Dictionary<int, int> executNumberCheck)

        {

            //没有需要刊登产品，则退出

            if (!products.Any())

            {

                return new ReturnedView();

            }

            if (autoPublishStrategy.View.Id == 30577)

            {

                Info($"{autoPublishStrategy.View.Id}开始刊登,数量{products.Count()}");

            }

            //公共

            var strategyView = autoPublishStrategy.View;

            var marketId = strategyView.MarketId;

            var strategyId = strategyView.Id;

            var customerId = strategyView.CustomerId;

            var orderSources = autoPublishStrategy.OrderSourceViews;

            var marketSites = autoPublishStrategy.MarketSiteViews;

            var skusByTemp = products.SelectMany(m => m.Variations.Select(s => s.SellerSku)).ToList();

            var titleInfoUpdate = new List<TitleInfo>();//回更新标题V3的集合



            //158商户才能取多套资料

            var enableErpMultiData = customerId == 1;



            List<ProductEditResponseModel> multiDataViews = null;

            if (enableErpMultiData == true)

            {

                //获取产品多套资料数据

                // multiDataViews = _baseApiService.GetMultiDatas(customerId, skusByTemp);

                //获取最少使用的多套资料

                var marketSiteCode = marketSites.FirstOrDefault()?.Code;

                var ReqquestView = new BatchGetMultiDataNumOfUsesForPublicRequest_ViewModel()

                {

                    CustomerId = customerId,

                    MarketId = marketId,

                    Skus = skusByTemp,

                    SiteCode = marketSiteCode

                };

                var returnedMDOrderNumList = _baseApiService.BatchGetMultiDataNumOfUsesForPublic(ReqquestView);



                if (returnedMDOrderNumList == null || returnedMDOrderNumList.Status != ReturnedStatusView.Success)

                {

                    Warning($"Mercadolibre刊登 -> 调用多套方法BatchGetMultiDataNumOfUsesForPublic失败，入参{JsonConvert.SerializeObject(ReqquestView)}; 返回值：{JsonConvert.SerializeObject(returnedMDOrderNumList)} ");

                    return new ReturnedView();

                }

                var orderNumList = returnedMDOrderNumList.Data;



                //获取产品多套资料数据

                multiDataViews = _baseApiService.GetMultiDatas(customerId, orderNumList);



                //修复最大套数

                var batchFixInputSkuUses = multiDataViews.Select(p => new BatchFixMultiDataMaxNumOfUsesForPublicRequest_ViewModel()

                {

                    Sku = p.Sku,

                    MaxOrderNum = p.FinishNum

                }).ToList();



                _baseApiService.BatchFixMultiDataMaxNumOfUsesForPublic(new CM_BatchFixMultiDataMaxNumOfUsesForPublicRequest_ViewModel()

                {

                    CustomerId = customerId,

                    MarketId = marketId,

                    SiteCode = marketSiteCode,

                    SkuUses = batchFixInputSkuUses

                });



            }



            foreach (var os in orderSources)

            {

                //线程池已取消当前Job通知

                if (jobToken.IsCancellationRequested)

                {

                    break;

                }

                var orderSource = os.View;

                var orderSourceId = orderSource.OrderSourceId;



                //店铺超额

                if (limitOrderSources.Contains(orderSourceId))

                {

                    break;

                }



                //店铺超限24小时

                if (_publishConsumer.GetMarketErrorCount($"Mercadolibre_{orderSourceId}") > 0)

                {

                    break;

                }

                //初始计数

                if (!executNumberCheck.ContainsKey(orderSourceId))

                {

                    executNumberCheck.Add(orderSourceId, 0);

                }



                //系统SKU

                var skus = products.SelectMany(m => m.Variations.Select(s => s.SellerSku)).ToList();

                //过滤刊登成功过的SKU

                _baseApiService.SetFilterPublishedSuccessSku(strategyView, orderSourceId, ref skus);

                //是否过来刊登过的SKU

                _baseApiService.SetFilterPublishedSku(strategyView, orderSourceId, ref skus);

                //过滤审核不通过的sku

                //if (!_newCategoryOrderSourceIds.Contains(orderSource.OrderSourceId))

                //{

                  //  _baseApiService.SetFilterProductReviewSku(strategyView.CustomerId, strategyView.MarketId, ref skus, orderSource.OrderSourceCountry);

                //}

                //过滤否定策略SKU

                _baseApiService.SetFilterNegationSku(strategyView.CustomerId, strategyView.MarketId, ref skus);

                //过滤在线listing数量超过策略限制的sku

                _baseApiService.SetFilterOnlineListingCountSku(strategyView, ref skus, orderSource.OrderSourceCountry);

                #region 获取价格列表

                // 市场价格字典

                Dictionary<string, List<PriceResponseModel>> marketPriceValues = new Dictionary<string, List<PriceResponseModel>>();



                var marketCategoryResult = _mercadolibreListingClientService.GetMarketCategorys(skus);

                var categoorys = new List<M_Mercadolibre_Sku_MarketCategory>();

                if (marketCategoryResult?.Data != null && marketCategoryResult.Status == ReturnedStatusView.Success)

                {

                    categoorys = marketCategoryResult.Data;

                }



                //默认巴西价格

                int calculationFormulaId = strategyView.CalculationFormulaId ?? 0;

                var brPriceModels = new List<PriceResponseModel>();

                if (strategyView.PublishToSiteCodes.Contains("MLB"))

                {

                    var s1 = _baseApiService.RetryTwoHandler(() =>

                    {

                        brPriceModels = GetPriceResponseModels(customerId, orderSourceId, calculationFormulaId, "BR", skus);

                        if (brPriceModels.Any())

                        {

                            return new RetryHandleView<string> { Response = "success" };

                        }

                        else

                        {

                            return null;

                        }

                    });

                    marketPriceValues.Add("MLB", brPriceModels);

                }



                //墨西哥

                var mxPriceModels = new List<PriceResponseModel>();

                if (strategyView.PublishToSiteCodes.Contains("MLM"))

                {

                    var s1 = _baseApiService.RetryTwoHandler(() =>

                    {

                        mxPriceModels = GetPriceResponseModels(customerId, orderSourceId, calculationFormulaId, "MX", skus);

                        if (mxPriceModels.Any())

                        {

                            return new RetryHandleView<string> { Response = "success" };

                        }

                        else

                        {

                            return null;

                        }

                    });

                    marketPriceValues.Add("MLM", mxPriceModels);

                }



                //智利

                var clPriceModels = new List<PriceResponseModel>();

                if (strategyView.PublishToSiteCodes.Contains("MLC"))

                {

                    var s1 = _baseApiService.RetryTwoHandler(() =>

                    {

                        clPriceModels = GetPriceResponseModels(customerId, orderSourceId, calculationFormulaId, "CL", skus);

                        if (clPriceModels.Any())

                        {

                            return new RetryHandleView<string> { Response = "success" };

                        }

                        else

                        {

                            return null;

                        }

                    });

                    marketPriceValues.Add("MLC", clPriceModels);

                }



                //哥伦比亚

                var coPriceModels = new List<PriceResponseModel>();

                if (strategyView.PublishToSiteCodes.Contains("MCO"))

                {

                    var s1 = _baseApiService.RetryTwoHandler(() =>

                    {

                        coPriceModels = GetPriceResponseModels(customerId, orderSourceId, calculationFormulaId, "CO", skus);

                        if (coPriceModels.Any())

                        {

                            return new RetryHandleView<string> { Response = "success" };

                        }

                        else

                        {

                            return null;

                        }

                    });

                    marketPriceValues.Add("MCO", coPriceModels);

                }

                #endregion

                //语言

                var marketSite = marketSites.FirstOrDefault(m => m.Code == orderSource.OrderSourceCountry);

                var language = marketSite?.Language;

                #region 批量获取未使用的多套资料套数

                var multiDataViewsByLanguage = _baseApiService.GetMultiDataByLanguage(multiDataViews, new List<string>() { language });//指定语言的多套资料列表

                //var multiDataOrderNumsByAllUnused = new List<Ap_AutoPublish_MultiData_Use_Occupy_Io_ViewModel>();//未使用多套套数信息

                //var skusByOrderSourceId = skus;

                //if (enableErpMultiData)

                //{

                //    multiDataOrderNumsByAllUnused = _baseApiService.GetMultaiDataOrderNumsByAllUnused(marketSite, skusByOrderSourceId, multiDataViews, out multiDataViewsByLanguage, orderSourceId, strategyId);

                //}

                #endregion

                var titleAfterContentlength = _baseApiService.GetTitleAfterContentlengthByStrategyGenerateTitles(orderSource, strategyView);



                //自动刊登队列

                var autoPublishViews = new List<Cm_AutoPublish_ViewModel>();

                //刊登明细列表

                var publishDetailViews = new List<PublishDetailModel<ProductRequestModel>>();

                //过滤已经刊登产品

                var publishProducts = _baseApiService.CopyObject(products.Where(m =>

                    m.Variations.Any() && m.Variations.All(s => skus.Contains(s.SellerSku))).ToList());



                var failureAutoPublishViews = new List<Cm_AutoPublish_ViewModel>();

                //刊登





                foreach (var p in publishProducts)

                {

                    //线程池已取消当前Job通知

                    if (jobToken.IsCancellationRequested)

                    {

                        break;

                    }



                    //每次刊登数量限制

                    if ((strategyView.DailyHourPublishs.HasValue &&

                         executNumberCheck[orderSourceId] >= strategyView.DailyHourPublishs.Value))

                    {

                        if (!limitOrderSources.Contains(orderSourceId))

                        {

                            limitOrderSources.Add(orderSourceId);

                        }

                        break;

                    }

                    if (limitOrderSources.Contains(orderSourceId))

                    {

                        break;

                    }



                    //系统SKU

                    var sku = p.Variations.FirstOrDefault()?.SellerSku;



                    #region 记录不满足条件失败队列

                    var failureUniqueId = Guid.NewGuid().GuidToLong();

                    var failureAutoPublishView = new Cm_AutoPublish_ViewModel

                    {

                        View = new Ap_AutoPublish_Io_ViewModel

                        {

                            UniqueId = failureUniqueId,

                            CustomerId = customerId,

                            MarketId = marketId,

                            StrategyId = strategyId,

                            OrderSourceId = orderSourceId,

                            Type = 1,

                            Sku = sku,

                            AllSkus = JsonConvert.SerializeObject(new List<string> { sku }),

                            ClientSku = string.Empty,

                            //ParentSku = l.ParentSku,

                            SellerSku = string.Empty,

                            SiteCode = marketSite.Code,

                            Price = 0,

                            IsAgainPublish = false,

                            Status = "failure",

                            Date = Convert.ToInt32(DateTime.Now.ToString("yyyyMMdd")),

                            Time = DateTime.Now,

                            TaskStatus = 0

                        },

                        AllSkuViews = new List<string> { sku }.Select(m =>

                        {

                            return new Ap_AutoPublish_AllSku_Io_ViewModel

                            {

                                UniqueId = failureUniqueId,

                                CustomerId = customerId,

                                MarketId = marketId,

                                StrategyId = strategyId,

                                OrderSourceId = orderSourceId,

                                Type = 1,

                                Sku = m,

                                SiteCode = marketSite.Code,

                                Date = Convert.ToInt32(DateTime.Now.ToString("yyyyMMdd")),

                                Time = DateTime.Now

                            };

                        }).ToList()

                    };

                    #endregion





                    if (autoPublishStrategy.View.Id == 30577)

                    {

                        Info($"{autoPublishStrategy.View.Id}开始入队列,SKU:{sku}");

                    }

                    #region 默认巴西价格



                    var priceRecorder = new StringBuilder(255);



                    var price = brPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price ?? default;

                    if (price <= 0)

                    {

                        priceRecorder.Append($"MLB PRICE = {price}\n");

                        price = mxPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price ?? default;

                    }

                    if (price <= 0)

                    {

                        priceRecorder.Append($"MLM PRICE = {price}\n");

                        price = clPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price ?? default;

                    }

                    if (price <= 0)

                    {

                        priceRecorder.Append($"MLC PRICE = {price}\n");

                        price = coPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price ?? default;

                    }

                    if (price <= 0)

                    {

                        failureAutoPublishView.View.DelType = 1;

                        failureAutoPublishView.View.FailureReason = $"不满足条件:价格计算器获取异常，{priceRecorder}";

                        failureAutoPublishViews.Add(failureAutoPublishView);

                        continue;

                    }

                    p.Price = _baseApiService.GetSellingPrice(strategyView, price);

                    #endregion



                    var mxPriceModel = mxPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price;

                    var mxPrice = mxPriceModel ?? default;



                    var clPriceModel = clPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price;

                    var clPrice = clPriceModel ?? default;



                    var coPriceModel = coPriceModels.FirstOrDefault(m => m.Sku == sku)?.Price;

                    var coPrice = coPriceModel ?? default;



                    //是否满足刊登条件

                    p.Variations = p.Variations.Where(skuView =>

                    {

                        //是否能够刊登

                        var isCanAutoPublish = new ReturnedView();

                        var isMxCanAutoPublish = new ReturnedView();

                        var isClCanAutoPublish = new ReturnedView();

                        if (brPriceModels.Any() && strategyView.PublishToSiteCodes.Contains("MLB"))

                        {

                            isCanAutoPublish = _baseApiService.IsCanAutoPublish(strategyView, p.Price);

                        }

                        if (mxPriceModels.Any() && strategyView.PublishToSiteCodes.Contains("MLM"))

                        {

                            isMxCanAutoPublish = _baseApiService.IsCanAutoPublish(strategyView, mxPrice);

                        }

                        if (clPriceModels.Any() && strategyView.PublishToSiteCodes.Contains("MLC"))

                        {

                            isClCanAutoPublish = _baseApiService.IsCanAutoPublish(strategyView, clPrice);

                        }

                        if (coPriceModels.Any() && strategyView.PublishToSiteCodes.Contains("MCO"))

                        {

                            isClCanAutoPublish = _baseApiService.IsCanAutoPublish(strategyView, coPrice);

                        }

                        return isCanAutoPublish.Status == ReturnedStatusView.Success &&

                               isMxCanAutoPublish.Status == ReturnedStatusView.Success;

                    }).ToList();



                    if (!p.Variations.Any())

                    {

                        //价格不满足业务设置的策略条件

                        continue;

                    }



                    //赋值渠道SKU和价格

                    var sellerSkuModels = new List<GetOrderSourceSkuResponseModel>();

                    p.Variations.ForEach(skuView =>

                    {

                        //系统Sku

                        var skuCode = skuView.SellerSku;

                        //数量

                        var inventoryModel = inventoryModels.FirstOrDefault(m => m.Sku.Equals(skuCode));

                        var inventory = inventoryModel?.GoodNum ?? default;

                        var quantity = _baseApiService.GetQuantity(strategyView, inventory);

                        skuView.AvailableQuantity = quantity;

                        //获取售价

                        skuView.Price = _baseApiService.GetSellingPrice(strategyView, price);

                        //生成渠道Sku

                        var orderSourceSkuModels = _baseApiService.RetryTwoHandler(() =>

                        {

                            var s1 = _baseApiService.GenerateSellerSkus(customerId, orderSourceId, new List<string> { skuCode }, PublishType.AutoPublish, orderSource.SaleAdminId, orderSource.SaleAdminName, Markets.Mercadolibre.ToString());

                            if (s1.Any())

                            {

                                return new RetryHandleView<List<GetOrderSourceSkuResponseModel>> { Response = s1 };

                            }

                            else

                            {

                                return null;

                            }

                        });

                        var sellerSkuModel = orderSourceSkuModels.FirstOrDefault(m => m.Sku.Equals(skuCode));

                        var sellerSku = sellerSkuModel?.OrderSourceSku;

                        skuView.SellerSku = string.Empty;

                        if (!string.IsNullOrEmpty(sellerSku))

                        {

                            skuView.SellerSku = sellerSku;

                        }



                        sellerSkuModels.AddRange(orderSourceSkuModels);

                    });



                    //没有生成渠道sku则过滤掉

                    if (p.Variations.Any(a => string.IsNullOrEmpty(a.SellerSku)))

                    {

                        failureAutoPublishView.View.DelType = 1;

                        failureAutoPublishView.View.FailureReason = "不满足条件:渠道SKU获取失败，请检查";

                        failureAutoPublishViews.Add(failureAutoPublishView);

                        continue;

                    }



                    //第一个产品

                    var firstSkuView = p.Variations.FirstOrDefault();



                    string name = string.Empty;

                    //158商户才会走这里

                    // Ap_AutoPublish_MultiData_Use_Occupy_Io_ViewModel currMultiDataOccupy = null;

                    var currMultiDataDetail = _baseApiService.GetEbayMultiDataBySku(multiDataViewsByLanguage, sku)?.ProductDetail?.FirstOrDefault(); // 当前使用的多套资料

                    var isManual = (int)ProductManualType.All;//多套资料标签属性



                    #region 新标题库取值V3

                    string _err = string.Empty;

                    string titleId = string.Empty;

                    List<SkuTitleModel> skuTitleModelList = null;

                    var productNameV3 = string.Empty; //新标题库标题 

                    var ruleId = 0;//规则ID

                    (productNameV3, titleId, skuTitleModelList, _err, ruleId) = _baseApiService.GetPublishTitleV3(new Cm_TitleRequsetView

                    {

                        Title = productNameV3,

                        Sku = sku,

                        MarketSite = marketSite,

                        MultiDataViewsByLanguage = multiDataViewsByLanguage,

                        TitleAddedContentlength = titleAfterContentlength,

                        VariantSkus = new List<string> { sku },

                        OrderSourceId = orderSourceId

                    });

                    #endregion





                    if (enableErpMultiData == true)

                    {

                        //name = _baseApiService.GetTitle(new Cm_TitleRequsetView

                        //{

                        //    Title = name,

                        //    Sku = sku,

                        //    MarketSite = marketSite,

                        //    MultiDataViewsByLanguage = multiDataViewsByLanguage,

                        //    MultiDataOrderNumsByAllUnused = multiDataOrderNumsByAllUnused,

                        //    TitleAddedContentlength = titleAfterContentlength,

                        //    VariantSkus = new List<string> { sku },

                        //}, out currMultiDataOccupy, out currMultiDataDetail);



                        //多套资料内容

                        if (currMultiDataDetail != null)

                        {

                            isManual = currMultiDataDetail.IsManual ?? 0;

                            name = currMultiDataDetail.ProductName;

                        }

                    }

                    else

                    {

                        if (string.IsNullOrEmpty(productNameV3))

                        {

                            //生成标题

                            var titleModels = _baseApiService.GenerateTitles(customerId, marketId, language, new List<string> { sku });

                            var titleModel = titleModels.FirstOrDefault(m => m.Sku == sku);

                            name = Left(titleModel?.Title, 60);

                        }

                    }



                    //优先取新标题库V3

                    if (!string.IsNullOrEmpty(productNameV3))

                    {

                        name = productNameV3;

                    }

                    else

                    {

                        if (string.IsNullOrEmpty(name))

                        {

                            //没规则取旧标题库

                            var titleModels = _baseApiService.RetryExceptionHandler((int)Markets.Allegro, () =>

                            {

                                var result = _baseApiService.GenerateTitles(customerId, marketId, language, new List<string> { sku });

                                return new RetryHandleView<List<ProductTitleResponseModel>>

                                {

                                    Code = "success",

                                    Response = result

                                };

                            });

                            if (titleModels.Any())

                            {

                                name = titleModels.FirstOrDefault().Title;

                            }

                            else

                            {

                                failureAutoPublishView.View.FailureReason = "旧标题获取失败";

                            }

                        }

                    }

                    

                    //标题库标题如果没有数据就使用多套资料标题（多套资料标题已赋值给Title）

                    if (!string.IsNullOrEmpty(name))

                    {

                        p.Title = _baseApiService.GenerateTitlesByStrategy(marketSite, orderSource, strategyView, name, p.Title);

                    }

                    else

                    {

                        failureAutoPublishView.View.FailureReason += "不满足条件:标题，请检查";

                        failureAutoPublishViews.Add(failureAutoPublishView);

                        continue;

                    }



                    //过滤敏感词

                    if (customSensitiveKeywordsViews.Any())

                    {

                        var returnFilterSensitive = _baseApiService.FilterCustomSensitiveKeywords(customSensitiveKeywordsViews, new Cm_Custom_Sensitive_Keywords_Io_ViewModel

                        {

                            Title = p.Title,

                            PlatformId = 18,

                            CategoryPathId = categoorys.FirstOrDefault(n => n.Sku == sku)?.AmazonPathNodeId

                        });

                        if (returnFilterSensitive != null && returnFilterSensitive.Status == ReturnedStatusView.Error)

                        {

                            failureAutoPublishView.View.DelType = 1;

                            failureAutoPublishView.View.FailureReason = "敏感词接口调用失败,请检查";

                            failureAutoPublishViews.Add(failureAutoPublishView);

                            continue;

                        }

                        if (string.IsNullOrWhiteSpace(returnFilterSensitive.Title))

                        {

                            failureAutoPublishView.View.FailureReason = "不满足条件:标题(过滤敏感词后)，请检查";

                            failureAutoPublishViews.Add(failureAutoPublishView);

                            continue;

                        }

                        p.Title = returnFilterSensitive.Title;

                    }



                    p.Attributes.Add(new ProductAttributeModel

                    {

                        Id = "SELLER_SKU",

                        ValueName = firstSkuView.SellerSku

                    });

                    var listSkuGroups = new List<SkuVariantGroupRelation>();

                    var productAutoPublishModel = views.FirstOrDefault(m => m.View.Sku == sku);

                    if (productAutoPublishModel == null)

                    {

                        views.Where(x => x.MarketProductViews.Any()).ForEach(v =>

                        {

                            var view = v.MarketProductViews[0].VariantProductViews.FirstOrDefault(n => n.Sku == sku);

                            if (view != null)

                            {

                                listSkuGroups.Add(new SkuVariantGroupRelation

                                {

                                    Sku = view.Sku,

                                    ParentId = view.IsVariant && !string.IsNullOrEmpty(view.ParentId) ? view.ParentId : "",

                                    ChildSKU = ""

                                });

                            }

                        });

                    }

                    else

                    {

                        listSkuGroups.Add(new SkuVariantGroupRelation

                        {

                            Sku = productAutoPublishModel.View.Sku,

                            ParentId = productAutoPublishModel.View.IsVariant && !string.IsNullOrEmpty(productAutoPublishModel.View.ParentId) ? productAutoPublishModel.View.ParentId : "",

                            ChildSKU = (productAutoPublishModel.View.GroupSkuId.ToLong() > 0L) && ((productAutoPublishModel.View.IsGroup ?? false) == true) ? productAutoPublishModel.GroupSkuViews.FirstOrDefault(m => m.GroupSku == sku)?.Sku : ""

                        });

                    }

                    //生成UPC

                    var upcs = _baseApiService.RetryTwoHandler(() =>

                    {

                        var s1 = _baseApiService.GenerateUpcs(customerId, marketId, orderSourceId, (int)UpcSourceType.AutoPublishBind, listSkuGroups, firstSkuView.SellerSku);

                        if (s1.Keys.Any())

                        {

                            return new RetryHandleView<Dictionary<string, string>> { Response = s1 };

                        }

                        else

                        {

                            return null;

                        }

                    });

                    if (!upcs.Any() || string.IsNullOrEmpty(upcs[sku]))

                    {

                        Info($"[MercadoLibre][GenerateUpcs] 警告：{orderSourceId} - {sku} - UPC无效");

                        failureAutoPublishView.View.DelType = 1;

                        failureAutoPublishView.View.FailureReason = "不满足条件:UPC获取失败，请检查";

                        failureAutoPublishViews.Add(failureAutoPublishView);

                        break;

                    }

                    var upcAttribute = p.Attributes.FirstOrDefault(m => m.Id == "GTIN");

                    upcAttribute.ValueName = upcs[sku];



                    //标准产品

                    loggerData.Sku = sku;

                    loggerData.SellerSku = firstSkuView.SellerSku;



                    //自定义SKU

                    var customSku = views.FirstOrDefault(m => m.View.Sku == sku)?.View.CustomSku;

                    //刊登队列

                    var uniqueId = Guid.NewGuid().GuidToLong();

                    var autoPublishView = new Cm_AutoPublish_ViewModel

                    {

                        View = new Ap_AutoPublish_Io_ViewModel

                        {

                            UniqueId = uniqueId,

                            CustomerId = customerId,

                            ItemId = String.Empty,

                            MarketId = marketId,

                            StrategyId = strategyId,

                            OrderSourceId = orderSourceId,

                            Type = (isVariation ? 2 : 1),

                            Sku = sku,

                            AllSkus = JsonConvert.SerializeObject(new List<string> { sku }),

                            ClientSku = customSku,

                            SellerSku = firstSkuView.SellerSku,

                            SiteCode = orderSource.OrderSourceCountry,

                            Price = firstSkuView.Price.ToDecimalOrDefault(),

                            IsAgainPublish = false,

                            Status = "normal",

                            Date = Convert.ToInt32(DateTime.Now.ToString("yyyyMMdd")),

                            Time = DateTime.Now,

                            Upc = upcs[sku],

                            TaskStatus = (int)QueueTaskStatus.NoLoadQueue,

                            IsManual = isManual,

                            RuleId = ruleId

                        },

                        AllSkuViews = new List<string> { sku }.Select(m => new Ap_AutoPublish_AllSku_Io_ViewModel

                        {

                            UniqueId = uniqueId,

                            CustomerId = customerId,

                            MarketId = marketId,

                            StrategyId = strategyId,

                            OrderSourceId = orderSourceId,

                            Type = (isVariation ? 2 : 1),

                            Sku = m,

                            SiteCode = string.Empty,

                            Date = Convert.ToInt32(DateTime.Now.ToString("yyyyMMdd")),

                            Time = DateTime.Now

                        }).ToList()

                    };

                    autoPublishViews.Add(autoPublishView);



                    #region 刊登明细

                    publishDetailViews.Add(new Mongo.Model.PublishDetailModel<ProductRequestModel>()

                    {

                        CustomerId = customerId,

                        OrderSourceId = orderSourceId,

                        Sku = autoPublishView.View.Sku,

                        SellerSku = autoPublishView.View.SellerSku,

                        StrategyId = autoPublishView.View.StrategyId,

                        QueueUniqueId = autoPublishView.View.UniqueId,

                        Detail = p

                    });



                    #endregion



                    #region 标题库V3

                    if (!string.IsNullOrEmpty(titleId))

                    {

                        titleInfoUpdate.Add(new TitleInfo

                        {

                            TitleId = titleId,

                            Language = "EN",

                            OrderSourceTypeId = (int)Markets.Mercadolibre,

                            OrderSourceSku = firstSkuView.SellerSku

                        });

                    }

                    #endregion



                    //计数

                    executNumberCheck[orderSourceId] += 1;

                }



                //记录失败原因

                if (failureAutoPublishViews.Any())

                {

                    foreach (var autoPublish in failureAutoPublishViews)

                    {

                        var autoPublishFailureReturned = _autoPublishClientService.Inserts(

                            new BatchInputView<Cm_AutoPublish_ViewModel>

                            {

                                User = GetUserView(customerId),

                                Views = new List<Cm_AutoPublish_ViewModel> { autoPublish }

                            });

                    }

                }



                if (publishDetailViews.Any())

                {

                    #region 更新标题库V3

                    if (titleInfoUpdate.Any())

                    {

                        var batchUpdateReturnedView = _baseApiService.BatchUpdateTitles(new BatchUpdateTitlesRequestModel()

                        {

                            TitleInfos = titleInfoUpdate

                        });

                        if (batchUpdateReturnedView == null || batchUpdateReturnedView.Status != ReturnedStatusView.Success)

                        {

                            loggerData.RequestParameter = titleInfoUpdate;

                            loggerData.ResponseParameter = batchUpdateReturnedView;

                            Info("[Mercadolibre][AutoPublish] BatchUpdateTitles 发生错误", loggerData);



                            Error($"新增自动刊登标题库V3失败");

                            var upcs = string.Join(",", autoPublishViews.Select(m => m.View.Upc).ToList());

                            Error($"[Mercadolibre UPC] 队列数据新增失败，请复用以下 UPC \n {upcs}");



                            continue;

                        }

                    }

                    #endregion



                    try

                    {

                        //插入刊登明细

                        _mercadolibreMongo.InsertMany(publishDetailViews);

                    }

                    catch (Exception ex)

                    {

                        autoPublishViews.ForEach(autoPublishView =>

                        {

                            autoPublishView.View.Status = "failure";

                            autoPublishView.View.FailureReason = "插入刊登队列出现异常：" + ex.Message;

                        });



                        loggerData.RequestParameter = new { publishDetailViews };

                        Info("[Mercadolibre][AutoPublish] InsertMany 发生错误", loggerData);

                        Error($"新增自动刊登明细失败");

                        var upcs = string.Join(",", autoPublishViews.Select(m => m.View.Upc).ToList());

                        Error($"[Mercadolibre UPC] 队列数据新增失败，请复用以下 UPC \n {upcs}");

                        continue;

                    }

                }



                if (autoPublishViews.Any())

                {

                    //批量新增刊登队列

                    autoPublishViews = autoPublishViews.Where(m => !(m.View.Status == "failure" && m.View.FailureReason.Contains("token"))).ToList();

                    var autoPublishReturned = _autoPublishClientService.Inserts(new BatchInputView<Cm_AutoPublish_ViewModel>

                    {

                        User = GetUserView(customerId),

                        Views = autoPublishViews

                    });

                    if (autoPublishReturned == null || autoPublishReturned.Status != ReturnedStatusView.Success)

                    {

                        Error($"新增自动刊登队列数据失败，{JsonConvert.SerializeObject(autoPublishReturned)}");

                        var upcs = string.Join(",", autoPublishViews.Select(m => m.View.Upc).ToList());

                        Error($"[Mercadolibre UPC] 队列数据新增失败，请复用以下 UPC \n {upcs}");

                        continue;

                    }

                    //MQ消息

                    if (strategyView.PriorityLevel is null ||strategyView.PriorityLevel == 0)

                    {

                        var messageEnableResult = _marketClientService.GetNewFunctionIsEnabled(new Sys_NewFunction_Config_Search_ViewModel

                        {

                            MarketId = marketId,

                            Type = "AutoPublish"

                        });

                        if (messageEnableResult is null || messageEnableResult.Status != ReturnedStatusView.Success) continue;

                        if (messageEnableResult.Data)

                        {

                            var messageId = Guid.NewGuid().ToString();

                            var autoPublishSku = autoPublishViews.Select(m => m.View.Sku).Distinct().ToList();

                            var autoPublishResult = _autoPublishClientService.GetAutoPublishViews(new Cm_AutoPublish_Search_ViewModel

                            {

                                CustomerId = customerId,

                                MarketId = (int)Markets.Mercadolibre,

                                PublishType = (int)PublishTypes.AutoPublish,

                                StrategyId = strategyId,

                                OrderSourceId = orderSourceId,

                                Skus = autoPublishSku

                            });

                            if (autoPublishResult is null || autoPublishResult.Status != ReturnedStatusView.Success) continue;



                            var syncAutoPublishes = autoPublishViews.Select(m =>

                            {

                                var autoPublishEntity = autoPublishResult.Data.FirstOrDefault(n => n.Sku == m.View.Sku);

                                return new M_Mercadolibre_SyncAutoPublish_Io_ViewModel

                                {

                                    AutoPublishId = autoPublishEntity?.Id ?? 0,

                                    AutoPublishStatus = 0,

                                    OrderSourceId = orderSourceId,

                                    MessageId = messageId,

                                    SendStatus = 0,

                                    SendTime = DateTime.Now,

                                    Status = 0,

                                    FailureLog = "N/A"

                                };

                            }).ToList();

                            var syncAutoPublishResult = _mercadolibreListingClientService.AddSyncAutoPublish(new InputBaseView<List<M_Mercadolibre_SyncAutoPublish_Io_ViewModel>>

                            {

                                Input = syncAutoPublishes

                            }).GetAwaiter().GetResult();

                            if (syncAutoPublishResult is null || syncAutoPublishResult.Status != ReturnedStatusView.Success)

                            {

                                //添加5次重试

                                for (int i = 0; i < 5; i++)

                                {

                                    syncAutoPublishResult = _mercadolibreListingClientService.AddSyncAutoPublish(new InputBaseView<List<M_Mercadolibre_SyncAutoPublish_Io_ViewModel>>

                                    {

                                        Input = syncAutoPublishes

                                    }).GetAwaiter().GetResult();

                                    if (syncAutoPublishResult.Status == ReturnedStatusView.Success)

                                    {

                                        break;

                                    }

                                }

                                if (syncAutoPublishResult is null || syncAutoPublishResult.Status != ReturnedStatusView.Success)

                                {

                                    continue;

                                }

                            }

                            var msgResult = _messagePublishService.SendMsg(

                                new AutoPublishMessage { MessageId = messageId }.ToMessageJson(),

                                new QueueBaseOption

                                {

                                    Queue = "Marketpublish-Cron-Mercadolibre-AutoPublish",

                                    Exchange = "Marketpublish-Cron-Mercadolibre-AutoPublish",

                                    ExchangeType = "direct",

                                    ConsumerTag = string.Empty,

                                    FetchCount = 1,

                                    RouteKey = "AutoPublish"

                                });

                            if (msgResult == string.Empty)

                            {

                                syncAutoPublishes.ForEach(m =>

                                {

                                    m.SendStatus = 1;

                                    m.SendTime = DateTime.Now;

                                });

                                var updateSyncAutoPublishResult = _mercadolibreListingClientService.UpdateSyncAutoPublishStatus(syncAutoPublishes)

                                    .GetAwaiter()

                                    .GetResult();

                            }

                        }

                    }

                }

            }



            return new ReturnedView();

        }



        #endregion



        #region 获取包裹长宽高属性

        /// <summary>

        /// 

        /// </summary>

        /// <param name="productView"></param>

        protected List<ProductAttributeModel> GetProductAtrributes(P_Product_Io_ViewModel productView)

        {

            //包裹重

            var weight = _baseApiService.GetShippingWeight(productView.PackageWeight, productView.GrossWeight);

            var height = Convert.ToDecimal(productView.PackageHeight.ToString("G0"));

            var width = Convert.ToDecimal(productView.PackageWidth.ToString("G0"));

            var length = Convert.ToDecimal(productView.PackageLength.ToString("G0"));



            //先判断长宽高

            var heightTemp = height < 3 ? 3 : height;

            var widthTemp = width < 3 ? 3 : width;

            var lengthTemp = length < 3 ? 3 : length;

            //然后计算体积

            var volumn = heightTemp * widthTemp * lengthTemp;

            //如果体积小于1000,则调整尺寸刊登

            if (volumn < 1000 && volumn >= 500)

            {

                height = heightTemp;

                width = widthTemp;

                length = lengthTemp;

            }

            else if (volumn < 500)

            {

                double pow = Double.Parse((500 / volumn).ToString());

                var multBei = Decimal.Parse(Math.Pow(pow, 1.0 / 3).ToString());

                height = Math.Ceiling(multBei * heightTemp);

                width = Math.Ceiling(multBei * widthTemp);

                length = Math.Ceiling(multBei * lengthTemp);

            }



            //重量不足70g设置位70g

            weight = weight < 0.07m ? 0.07m : weight;



            var attributes = new List<ProductAttributeModel>();

            attributes.Add(new ProductAttributeModel

            {

                Id = "PACKAGE_WEIGHT",

                ValueName = $"{weight} kg"

            });

            attributes.Add(new ProductAttributeModel

            {

                Id = "PACKAGE_LENGTH",

                ValueName = $"{length} cm"

            });

            attributes.Add(new ProductAttributeModel

            {

                Id = "PACKAGE_WIDTH",

                ValueName = $"{width} cm"

            });

            attributes.Add(new ProductAttributeModel

            {

                Id = "PACKAGE_HEIGHT",

                ValueName = $"{height} cm"

            });



            return attributes;

        }



        #endregion



        /// <summary>

        /// 报错原因

        /// </summary>

        /// <param name="error"></param>

        /// <param name="cause"></param>

        /// <param name="message"></param>

        /// <returns></returns>

        protected string GetErrorMessage(string error, string message, dynamic cause)

        {

            var failureReason = string.Empty;

            failureReason += error;

            failureReason += message;

            if (cause != null && !string.IsNullOrEmpty(JsonConvert.SerializeObject(cause)))

                failureReason += JsonConvert.SerializeObject(cause);

            return failureReason;

        }



        /// <summary>

        /// 获取价格

        /// </summary>

        /// <param name="customerId"></param>

        /// <param name="orderSourceId"></param>

        /// <param name="country"></param>

        /// <param name="skus"></param>

        /// <returns></returns>

        protected List<PriceResponseModel> GetPriceResponseModels(int customerId, int orderSourceId, int calculationFormulaid, string country, List<string> skus)

        {

            return _baseApiService.GetPrices(new GetPriceRequestModel

            {

                CustomerId = customerId,

                BusinessTypeCode = "MercadolibreLocalWarehouse",

                PlatformId = (int)Markets.Mercadolibre,

                WarehouseTypeId = 1,

                CalculationFormulaId = calculationFormulaid,

                WarehouseId = 5312,

                OrderSourceId = orderSourceId,

                Country = country,

                Skus = skus

            });

        }



    }

}

